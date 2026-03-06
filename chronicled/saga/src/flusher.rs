use crate::config::SagaConfig;
use crate::error::Result;
use crate::memtable::FrozenBuffer;
use crate::saga_catalog::SagaCatalog;
use crate::types::L0FileMeta;
use arrow::array::RecordBatch;
use arrow::compute::{concat_batches, sort_to_indices, take};
use arrow::datatypes::SchemaRef;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs;
use std::sync::Arc;
use tracing::info;

pub struct Flusher {
    config: SagaConfig,
    catalog: Arc<SagaCatalog>,
}

impl Flusher {
    pub fn new(config: SagaConfig, catalog: Arc<SagaCatalog>) -> Self {
        Self { config, catalog }
    }

    /// Flush a FrozenBuffer: merge batches, sort, write to local Parquet.
    pub async fn flush(&self, frozen: FrozenBuffer) -> Result<L0FileMeta> {
        let topic = &frozen.topic;
        let topic_config = self.catalog.topic_config(topic)?;
        let schema = frozen.schema.clone();

        // Merge all RecordBatches into one.
        let merged = concat_batches(&schema, &frozen.batches)?;

        // Sort by sort_keys.
        let sorted = sort_batches(merged, &schema, &topic_config.sort_keys)?;

        // Determine partition key from first timestamp column.
        let partition_key = determine_partition_key(&sorted, &topic_config);

        // Build output path.
        let dir = format!("{}/{}/l0", self.config.local_data_dir, topic);
        fs::create_dir_all(&dir)?;
        let filename = format!("part-{}.parquet", uuid::Uuid::new_v4());
        let path = format!("{}/{}", dir, filename);

        // Write Parquet file.
        let props = build_writer_properties(&schema, &[&sorted], &self.config);
        let file = fs::File::create(&path)?;
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
        writer.write(&sorted)?;
        writer.close()?;

        let file_size = fs::metadata(&path)?.len() as usize;

        // Compute timestamp range.
        let (min_ts, max_ts) = timestamp_range(&sorted, &topic_config.sort_keys);

        let meta = L0FileMeta {
            path: path.clone(),
            topic: topic.clone(),
            partition_key,
            row_count: sorted.num_rows(),
            file_size,
            min_timestamp: min_ts,
            max_timestamp: max_ts,
            min_sequence_id: frozen.min_sequence_id,
            max_sequence_id: frozen.max_sequence_id,
            created_at: chrono::Utc::now(),
        };

        self.catalog.add_l0_file(meta.clone());

        info!(
            topic = topic,
            rows = sorted.num_rows(),
            file_size,
            path = path,
            "L0 parquet flushed"
        );

        Ok(meta)
    }
}

/// Merge and sort a RecordBatch by the given sort key columns.
fn sort_batches(
    batch: RecordBatch,
    schema: &SchemaRef,
    sort_keys: &[String],
) -> Result<RecordBatch> {
    if batch.num_rows() == 0 || sort_keys.is_empty() {
        return Ok(batch);
    }

    // Find the first sort key column that exists.
    let sort_col_name = &sort_keys[0];
    let col_idx = match schema.index_of(sort_col_name) {
        Ok(idx) => idx,
        Err(_) => return Ok(batch), // Sort key not found; return unsorted.
    };

    let sort_col = batch.column(col_idx);
    let options = arrow::compute::SortOptions {
        descending: false,
        nulls_first: false,
    };
    let indices = sort_to_indices(sort_col, Some(options), None)?;

    let sorted_columns: Vec<Arc<dyn arrow::array::Array>> = batch
        .columns()
        .iter()
        .map(|col| take(col.as_ref(), &indices, None).map(Arc::from))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(RecordBatch::try_new(schema.clone(), sorted_columns)?)
}

/// Determine partition key from the first row's timestamp.
fn determine_partition_key(
    batch: &RecordBatch,
    topic_config: &crate::types::TopicConfig,
) -> String {
    use arrow::array::TimestampMillisecondArray;

    for key in &topic_config.sort_keys {
        if let Ok(idx) = batch.schema().index_of(key) {
            if let Some(ts_arr) = batch.column(idx).as_any().downcast_ref::<TimestampMillisecondArray>() {
                if !ts_arr.is_empty() {
                    return crate::types::partition_key(
                        ts_arr.value(0),
                        &topic_config.partition_granularity,
                    );
                }
            }
        }
    }
    crate::types::partition_key(0, &topic_config.partition_granularity)
}

/// Extract min/max timestamp from the batch.
fn timestamp_range(batch: &RecordBatch, sort_keys: &[String]) -> (i64, i64) {
    use arrow::array::TimestampMillisecondArray;
    use arrow::compute::{min, max};

    for key in sort_keys {
        if let Ok(idx) = batch.schema().index_of(key) {
            if let Some(ts_arr) = batch.column(idx).as_any().downcast_ref::<TimestampMillisecondArray>() {
                let min_val = min(ts_arr).unwrap_or(0);
                let max_val = max(ts_arr).unwrap_or(0);
                return (min_val, max_val);
            }
        }
    }
    (0, 0)
}

/// Build Parquet writer properties with encoding strategies per data type.
fn build_writer_properties(
    schema: &SchemaRef,
    _batches: &[&RecordBatch],
    config: &SagaConfig,
) -> WriterProperties {
    use arrow::datatypes::DataType;

    let mut builder = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_max_row_group_size(config.parquet_row_group_size)
        .set_data_page_size_limit(1024 * 1024) // 1MB data pages
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page);

    for field in schema.fields() {
        let col_path = parquet::schema::types::ColumnPath::new(vec![field.name().clone()]);
        builder = match field.data_type() {
            DataType::Int64 | DataType::Timestamp(_, _) => {
                builder.set_column_encoding(col_path, Encoding::DELTA_BINARY_PACKED)
            }
            DataType::Float64 => {
                builder.set_column_encoding(col_path, Encoding::BYTE_STREAM_SPLIT)
            }
            DataType::Boolean => {
                builder.set_column_encoding(col_path, Encoding::RLE)
            }
            DataType::Utf8 => {
                // Default to dictionary; merger can refine.
                builder.set_column_encoding(col_path, Encoding::RLE_DICTIONARY)
            }
            _ => builder,
        };
    }

    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{FieldDef, FieldType, PartitionGranularity, TopicConfig};
    use arrow::array::{Int64Array, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    #[test]
    fn sort_batches_basic() {
        let schema = test_schema();
        let ts = Arc::new(TimestampMillisecondArray::from(vec![300, 100, 200]));
        let val = Arc::new(Int64Array::from(vec![3, 1, 2]));
        let batch = RecordBatch::try_new(schema.clone(), vec![ts, val]).unwrap();

        let sorted = sort_batches(batch, &schema, &["timestamp".into()]).unwrap();
        let ts_col = sorted.column(0).as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
        assert_eq!(ts_col.value(0), 100);
        assert_eq!(ts_col.value(1), 200);
        assert_eq!(ts_col.value(2), 300);
    }

    #[tokio::test]
    async fn flush_creates_parquet_file() {
        let dir = tempfile::tempdir().unwrap();
        let config = SagaConfig {
            local_data_dir: dir.path().to_string_lossy().to_string(),
            ..Default::default()
        };
        let catalog = Arc::new(SagaCatalog::new());
        catalog.register_topic(TopicConfig {
            name: "test".into(),
            schema: vec![
                FieldDef { name: "timestamp".into(), data_type: FieldType::TimestampMillis, nullable: false },
                FieldDef { name: "value".into(), data_type: FieldType::Int64, nullable: true },
            ],
            sort_keys: vec!["timestamp".into()],
            partition_granularity: PartitionGranularity::Day,
        }).unwrap();

        let schema = test_schema();
        let ts = Arc::new(TimestampMillisecondArray::from(vec![200, 100]));
        let val = Arc::new(Int64Array::from(vec![2, 1]));
        let batch = RecordBatch::try_new(schema.clone(), vec![ts, val]).unwrap();

        let frozen = FrozenBuffer {
            topic: "test".into(),
            schema,
            batches: vec![batch],
            min_sequence_id: 1,
            max_sequence_id: 1,
            total_rows: 2,
        };

        let flusher = Flusher::new(config, catalog.clone());
        let meta = flusher.flush(frozen).await.unwrap();
        assert_eq!(meta.row_count, 2);
        assert!(std::path::Path::new(&meta.path).exists());
        assert_eq!(catalog.all_l0_files("test").len(), 1);
    }
}
