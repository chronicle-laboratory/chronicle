use crate::config::SagaConfig;
use crate::error::Result;
use crate::storage::saga_catalog::SagaCatalog;
use crate::types::{L0FileMeta, L1FileMeta};
use arrow::array::RecordBatch;
use arrow::compute::{concat_batches, sort_to_indices, take};
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs;
use std::sync::Arc;
use tracing::info;

pub struct MergeResult {
    pub topic: String,
    pub partition: String,
    pub l0_files_merged: usize,
    pub l1_files_created: usize,
    pub rows_total: usize,
    pub bytes_before: usize,
    pub bytes_after: usize,
}

pub struct Merger {
    config: SagaConfig,
    catalog: Arc<SagaCatalog>,
    store: Arc<dyn ObjectStore>,
}

impl Merger {
    pub fn new(config: SagaConfig, catalog: Arc<SagaCatalog>, store: Arc<dyn ObjectStore>) -> Self {
        Self { config, catalog, store }
    }

    /// Check all topics/partitions for merge eligibility and merge if needed.
    pub async fn check_and_merge(&self) -> Result<Option<MergeResult>> {
        for topic in self.catalog.list_topics() {
            for partition in self.catalog.l0_partitions(&topic) {
                let files = self.catalog.l0_files(&topic, &partition);
                if files.len() >= self.config.merge_file_threshold {
                    let result = self.merge_files(&topic, &partition, files).await?;
                    return Ok(Some(result));
                }
            }
        }
        Ok(None)
    }

    async fn merge_files(
        &self,
        topic: &str,
        partition: &str,
        files: Vec<L0FileMeta>,
    ) -> Result<MergeResult> {
        let topic_config = self.catalog.topic_config(topic)?;
        let schema = self.catalog.schema(topic)?;

        let bytes_before: usize = files.iter().map(|f| f.file_size).sum();

        // Read all L0 files into RecordBatches.
        let mut all_batches = Vec::new();
        for f in &files {
            let file = fs::File::open(&f.path)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let reader = builder.build()?;
            for batch_result in reader {
                all_batches.push(batch_result?);
            }
        }

        if all_batches.is_empty() {
            return Ok(MergeResult {
                topic: topic.into(),
                partition: partition.into(),
                l0_files_merged: files.len(),
                l1_files_created: 0,
                rows_total: 0,
                bytes_before,
                bytes_after: 0,
            });
        }

        // Merge and sort.
        let merged = concat_batches(&schema, &all_batches)?;
        let sorted = sort_batch(&merged, &schema, &topic_config.sort_keys)?;

        // Write merged Parquet to a temp local file.
        let merged_filename = format!("merged-{}.parquet", uuid::Uuid::new_v4());
        let local_dir = format!("{}/{}/merged", self.config.local_data_dir, topic);
        fs::create_dir_all(&local_dir)?;
        let local_path = format!("{}/{}", local_dir, merged_filename);

        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
            .set_max_row_group_size(self.config.parquet_row_group_size)
            .build();

        let file = fs::File::create(&local_path)?;
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
        writer.write(&sorted)?;
        writer.close()?;

        let file_size = fs::metadata(&local_path)?.len() as usize;
        let rows_total = sorted.num_rows();

        // Upload to object store.
        let s3_path = format!(
            "{}/{}/data/{}/{}",
            self.config.storage_root, topic, partition, merged_filename
        );
        let data = tokio::fs::read(&local_path).await?;
        let object_path = object_store::path::Path::from(s3_path.clone());
        self.store
            .put(&object_path, object_store::PutPayload::from(data))
            .await?;

        // Compute timestamp range.
        let (min_ts, max_ts) = timestamp_range(&sorted, &topic_config.sort_keys);

        let l1_meta = L1FileMeta {
            s3_path: s3_path.clone(),
            partition_key: partition.into(),
            row_count: rows_total,
            file_size,
            min_timestamp: min_ts,
            max_timestamp: max_ts,
        };

        // Update catalog: remove merged L0, add L1.
        let merged_paths: Vec<String> = files.iter().map(|f| f.path.clone()).collect();
        self.catalog.complete_merge(topic, &merged_paths, vec![l1_meta]);

        // Delete source L0 files and temp merged file.
        for f in &files {
            let _ = fs::remove_file(&f.path);
        }
        let _ = fs::remove_file(&local_path);

        info!(
            topic,
            partition,
            l0_merged = files.len(),
            rows = rows_total,
            bytes_before,
            bytes_after = file_size,
            "L0 → L1 merge complete"
        );

        Ok(MergeResult {
            topic: topic.into(),
            partition: partition.into(),
            l0_files_merged: files.len(),
            l1_files_created: 1,
            rows_total,
            bytes_before,
            bytes_after: file_size,
        })
    }
}

fn sort_batch(
    batch: &RecordBatch,
    schema: &arrow::datatypes::SchemaRef,
    sort_keys: &[String],
) -> Result<RecordBatch> {
    if batch.num_rows() == 0 || sort_keys.is_empty() {
        return Ok(batch.clone());
    }

    let col_idx = match schema.index_of(&sort_keys[0]) {
        Ok(idx) => idx,
        Err(_) => return Ok(batch.clone()),
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

fn timestamp_range(batch: &RecordBatch, sort_keys: &[String]) -> (i64, i64) {
    use arrow::array::TimestampMillisecondArray;
    use arrow::compute::{max, min};

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{FieldDef, FieldType, PartitionGranularity, TopicConfig};
    use arrow::array::{Int64Array, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use object_store::memory::InMemory;

    fn test_schema() -> arrow::datatypes::SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    fn test_topic_config() -> TopicConfig {
        TopicConfig {
            name: "test".into(),
            schema: vec![
                FieldDef { name: "timestamp".into(), data_type: FieldType::TimestampMillis, nullable: false },
                FieldDef { name: "value".into(), data_type: FieldType::Int64, nullable: true },
            ],
            sort_keys: vec!["timestamp".into()],
            partition_granularity: PartitionGranularity::None,
        }
    }

    #[tokio::test]
    async fn merge_l0_files() {
        let dir = tempfile::tempdir().unwrap();
        let config = SagaConfig {
            local_data_dir: dir.path().to_string_lossy().to_string(),
            storage_root: "test-root".into(),
            merge_file_threshold: 2,
            ..Default::default()
        };

        let catalog = Arc::new(SagaCatalog::new());
        catalog.register_topic(test_topic_config()).unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let schema = test_schema();

        // Create two L0 Parquet files.
        let l0_dir = dir.path().join("test/l0");
        fs::create_dir_all(&l0_dir).unwrap();

        for i in 0..2 {
            let path = l0_dir.join(format!("part-{}.parquet", i));
            let ts = Arc::new(TimestampMillisecondArray::from(vec![200 - i * 100, 100 - i * 100]));
            let val = Arc::new(Int64Array::from(vec![i as i64 * 10, i as i64 * 10 + 1]));
            let batch = RecordBatch::try_new(schema.clone(), vec![ts, val]).unwrap();

            let file = fs::File::create(&path).unwrap();
            let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();

            catalog.add_l0_file(L0FileMeta {
                path: path.to_string_lossy().to_string(),
                topic: "test".into(),
                partition_key: "dt=all".into(),
                row_count: 2,
                file_size: fs::metadata(&path).unwrap().len() as usize,
                min_timestamp: 0,
                max_timestamp: 200,
                min_sequence_id: i as u64,
                max_sequence_id: i as u64,
                created_at: chrono::Utc::now(),
            });
        }

        let merger = Merger::new(config, catalog.clone(), store);
        let result = merger.check_and_merge().await.unwrap().unwrap();
        assert_eq!(result.l0_files_merged, 2);
        assert_eq!(result.rows_total, 4);
        assert_eq!(catalog.all_l0_files("test").len(), 0);
        assert_eq!(catalog.l1_files("test").len(), 1);
    }
}
