use crate::error::Result;
use crate::storage::memtable::Memtable;
use crate::storage::saga_catalog::SagaCatalog;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::catalog_common::MemoryCatalogProvider;
use datafusion::catalog_common::MemorySchemaProvider;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

/// Execute a SQL query against hot (memtable) + cold (L0 Parquet) data.
pub async fn execute_sql(
    sql: &str,
    catalog: &SagaCatalog,
    memtables: &parking_lot::RwLock<HashMap<String, Arc<Memtable>>>,
) -> Result<Vec<RecordBatch>> {
    let ctx = build_session_context(catalog, memtables)?;
    let df = ctx.sql(sql).await?;
    let batches = df.collect().await?;
    info!(sql = sql, rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(), "query executed");
    Ok(batches)
}

fn build_session_context(
    catalog: &SagaCatalog,
    memtables: &parking_lot::RwLock<HashMap<String, Arc<Memtable>>>,
) -> Result<SessionContext> {
    let ctx = SessionContext::new();

    let schema_provider = Arc::new(MemorySchemaProvider::new());

    for topic_name in catalog.list_subjects() {
        let schema = match catalog.schema(&topic_name) {
            Ok(s) => s,
            Err(_) => continue,
        };

        let batches = collect_topic_batches(&topic_name, &schema, catalog, memtables);

        if batches.is_empty() {
            let table = MemTable::try_new(schema, vec![])?;
            schema_provider.register_table(topic_name, Arc::new(table))?;
        } else {
            let table = MemTable::try_new(schema, vec![batches])?;
            schema_provider.register_table(topic_name, Arc::new(table))?;
        }
    }

    let catalog_provider = Arc::new(MemoryCatalogProvider::new());
    catalog_provider.register_schema("public", schema_provider)?;
    ctx.register_catalog("saga", catalog_provider);

    // Set default catalog/schema so queries don't need fully qualified names.
    let _ = ctx.sql("SET datafusion.catalog.default_catalog = 'saga'");
    let _ = ctx.sql("SET datafusion.catalog.default_schema = 'public'");

    Ok(ctx)
}

/// Collect all available data for a topic: memtable snapshots + L0 file reads.
fn collect_topic_batches(
    topic: &str,
    schema: &SchemaRef,
    catalog: &SagaCatalog,
    memtables: &parking_lot::RwLock<HashMap<String, Arc<Memtable>>>,
) -> Vec<RecordBatch> {
    let mut all_batches = Vec::new();

    // Hot data from memtable.
    {
        let tables = memtables.read();
        if let Some(mt) = tables.get(topic) {
            all_batches.extend(mt.snapshot());
        }
    }

    // Cold data from L0 Parquet files.
    let l0_files = catalog.all_l0_files(topic);
    for f in l0_files {
        if let Ok(file) = std::fs::File::open(&f.path) {
            if let Ok(builder) = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file) {
                if let Ok(reader) = builder.build() {
                    for batch_result in reader {
                        if let Ok(batch) = batch_result {
                            // Ensure schema compatibility.
                            if batch.schema().as_ref() == schema.as_ref() {
                                all_batches.push(batch);
                            }
                        }
                    }
                }
            }
        }
    }

    all_batches
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PartitionGranularity;
    use arrow::array::{Int64Array, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    fn setup() -> (Arc<SagaCatalog>, Arc<parking_lot::RwLock<HashMap<String, Arc<Memtable>>>>) {
        let catalog = Arc::new(SagaCatalog::new());

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Int64, true),
        ]));

        catalog.register_subject_local(
            "events",
            schema.clone(),
            vec!["timestamp".into()],
            PartitionGranularity::None,
        ).unwrap();

        let mt = Arc::new(Memtable::new(schema.clone()));
        let ts = Arc::new(TimestampMillisecondArray::from(vec![100, 200, 300]));
        let val = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_new(schema, vec![ts, val]).unwrap();
        mt.ingest(batch, 1).unwrap();

        let memtables = Arc::new(parking_lot::RwLock::new(HashMap::new()));
        memtables.write().insert("events".into(), mt);

        (catalog, memtables)
    }

    #[tokio::test]
    async fn query_memtable_data() {
        let (catalog, memtables) = setup();
        let results =
            execute_sql("SELECT * FROM saga.public.events", &catalog, &memtables)
                .await
                .unwrap();
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn query_with_filter() {
        let (catalog, memtables) = setup();
        let results = execute_sql(
            "SELECT * FROM saga.public.events WHERE value > 1",
            &catalog,
            &memtables,
        )
        .await
        .unwrap();
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }
}
