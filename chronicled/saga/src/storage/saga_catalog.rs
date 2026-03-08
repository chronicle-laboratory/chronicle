use crate::error::{Result, SagaError};
use crate::types::{L0FileMeta, L1FileMeta, PartitionGranularity};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use chronicle_lexicon_client::LexiconClient;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

struct SubjectState {
    schema: SchemaRef,
    sort_keys: Vec<String>,
    partition_granularity: PartitionGranularity,
    l0_files: Vec<L0FileMeta>,
    l1_files: Vec<L1FileMeta>,
}

/// Manages subject metadata and file index for Saga.
///
/// Schemas are resolved from Lexicon (schema registry) on demand.
pub struct SagaCatalog {
    subjects: RwLock<HashMap<String, SubjectState>>,
    lexicon: Option<Arc<tokio::sync::RwLock<LexiconClient>>>,
}

impl SagaCatalog {
    pub fn new() -> Self {
        Self {
            subjects: RwLock::new(HashMap::new()),
            lexicon: None,
        }
    }

    pub fn with_lexicon(lexicon: LexiconClient) -> Self {
        Self {
            subjects: RwLock::new(HashMap::new()),
            lexicon: Some(Arc::new(tokio::sync::RwLock::new(lexicon))),
        }
    }

    /// Register a subject with an explicit schema (for testing or manual config).
    pub fn register_subject_local(
        &self,
        name: &str,
        schema: SchemaRef,
        sort_keys: Vec<String>,
        partition_granularity: PartitionGranularity,
    ) -> Result<()> {
        let mut subjects = self.subjects.write();
        if subjects.contains_key(name) {
            return Err(SagaError::Internal(format!(
                "subject '{}' already exists",
                name
            )));
        }
        subjects.insert(
            name.to_string(),
            SubjectState {
                schema,
                sort_keys,
                partition_granularity,
                l0_files: Vec::new(),
                l1_files: Vec::new(),
            },
        );
        Ok(())
    }

    /// Resolve a subject's Arrow schema — fetches from Lexicon if not cached locally.
    pub async fn resolve_schema(&self, subject: &str) -> Result<SchemaRef> {
        // Check local cache first.
        {
            let subjects = self.subjects.read();
            if let Some(state) = subjects.get(subject) {
                return Ok(state.schema.clone());
            }
        }

        // Fetch from Lexicon.
        let lexicon = self
            .lexicon
            .as_ref()
            .ok_or_else(|| SagaError::Internal("lexicon client not configured".into()))?;

        let client = lexicon.read().await;
        let (_record, arrow_schema) = client.get_schema(subject).await.map_err(|e| {
            SagaError::Internal(format!("lexicon lookup for '{}': {}", subject, e))
        })?;

        info!(subject = %subject, fields = arrow_schema.fields().len(), "resolved schema from lexicon");

        // Derive sort_keys: use first timestamp column if present.
        let sort_keys = infer_sort_keys(&arrow_schema);

        // Cache it.
        let mut subjects = self.subjects.write();
        subjects
            .entry(subject.to_string())
            .or_insert_with(|| SubjectState {
                schema: arrow_schema.clone(),
                sort_keys,
                partition_granularity: PartitionGranularity::Day,
                l0_files: Vec::new(),
                l1_files: Vec::new(),
            });

        Ok(arrow_schema)
    }

    /// Get cached schema (sync). Returns error if not yet resolved.
    pub fn schema(&self, subject: &str) -> Result<SchemaRef> {
        let subjects = self.subjects.read();
        subjects
            .get(subject)
            .map(|t| t.schema.clone())
            .ok_or_else(|| SagaError::SubjectNotFound(subject.into()))
    }

    /// Get sort keys for a subject.
    pub fn sort_keys(&self, subject: &str) -> Vec<String> {
        let subjects = self.subjects.read();
        subjects
            .get(subject)
            .map(|t| t.sort_keys.clone())
            .unwrap_or_default()
    }

    /// Get partition granularity for a subject.
    pub fn partition_granularity(&self, subject: &str) -> PartitionGranularity {
        let subjects = self.subjects.read();
        subjects
            .get(subject)
            .map(|t| t.partition_granularity.clone())
            .unwrap_or_default()
    }

    /// Refresh a subject's schema from Lexicon.
    pub async fn refresh_schema(&self, subject: &str) -> Result<SchemaRef> {
        let old_state = {
            let mut subjects = self.subjects.write();
            subjects.remove(subject)
        };

        let schema = self.resolve_schema_from_lexicon(subject).await?;
        let sort_keys = infer_sort_keys(&schema);

        let mut subjects = self.subjects.write();
        let entry = subjects
            .entry(subject.to_string())
            .or_insert_with(|| SubjectState {
                schema: schema.clone(),
                sort_keys,
                partition_granularity: PartitionGranularity::Day,
                l0_files: Vec::new(),
                l1_files: Vec::new(),
            });

        // Restore old file lists and config if they existed.
        if let Some(old) = old_state {
            entry.l0_files = old.l0_files;
            entry.l1_files = old.l1_files;
            entry.partition_granularity = old.partition_granularity;
        }

        Ok(schema)
    }

    async fn resolve_schema_from_lexicon(&self, subject: &str) -> Result<SchemaRef> {
        let lexicon = self
            .lexicon
            .as_ref()
            .ok_or_else(|| SagaError::Internal("lexicon client not configured".into()))?;

        let client = lexicon.read().await;
        let (_record, arrow_schema) = client.get_schema(subject).await.map_err(|e| {
            SagaError::Internal(format!("lexicon lookup for '{}': {}", subject, e))
        })?;

        Ok(arrow_schema)
    }

    pub fn add_l0_file(&self, meta: L0FileMeta) {
        let mut subjects = self.subjects.write();
        if let Some(state) = subjects.get_mut(&meta.topic) {
            state.l0_files.push(meta);
        }
    }

    pub fn l0_files(&self, subject: &str, partition: &str) -> Vec<L0FileMeta> {
        let subjects = self.subjects.read();
        subjects
            .get(subject)
            .map(|t| {
                t.l0_files
                    .iter()
                    .filter(|f| f.partition_key == partition)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn all_l0_files(&self, subject: &str) -> Vec<L0FileMeta> {
        let subjects = self.subjects.read();
        subjects
            .get(subject)
            .map(|t| t.l0_files.clone())
            .unwrap_or_default()
    }

    pub fn complete_merge(
        &self,
        subject: &str,
        merged_l0_paths: &[String],
        new_l1: Vec<L1FileMeta>,
    ) {
        let mut subjects = self.subjects.write();
        if let Some(state) = subjects.get_mut(subject) {
            state
                .l0_files
                .retain(|f| !merged_l0_paths.contains(&f.path));
            state.l1_files.extend(new_l1);
        }
    }

    pub fn l1_files(&self, subject: &str) -> Vec<L1FileMeta> {
        let subjects = self.subjects.read();
        subjects
            .get(subject)
            .map(|t| t.l1_files.clone())
            .unwrap_or_default()
    }

    pub fn list_subjects(&self) -> Vec<String> {
        self.subjects.read().keys().cloned().collect()
    }

    pub fn l0_partitions(&self, subject: &str) -> Vec<String> {
        let subjects = self.subjects.read();
        subjects
            .get(subject)
            .map(|t| {
                let mut parts: Vec<String> = t
                    .l0_files
                    .iter()
                    .map(|f| f.partition_key.clone())
                    .collect();
                parts.sort();
                parts.dedup();
                parts
            })
            .unwrap_or_default()
    }
}

/// Infer sort keys from schema: use the first timestamp column if present.
fn infer_sort_keys(schema: &SchemaRef) -> Vec<String> {
    for field in schema.fields() {
        if matches!(
            field.data_type(),
            DataType::Timestamp(TimeUnit::Millisecond, _)
                | DataType::Timestamp(TimeUnit::Microsecond, _)
        ) {
            return vec![field.name().clone()];
        }
    }
    vec![]
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("val", DataType::Int64, true),
        ]))
    }

    #[test]
    fn schema_not_found_without_lexicon() {
        let cat = SagaCatalog::new();
        assert!(cat.schema("nonexistent").is_err());
    }

    #[test]
    fn register_local_and_query() {
        let cat = SagaCatalog::new();
        cat.register_subject_local(
            "events",
            test_schema(),
            vec!["ts".into()],
            PartitionGranularity::Day,
        )
        .unwrap();
        assert_eq!(cat.list_subjects(), vec!["events".to_string()]);
        assert!(cat.schema("events").is_ok());
        assert_eq!(cat.sort_keys("events"), vec!["ts".to_string()]);
    }

    #[test]
    fn infer_sort_keys_finds_timestamp() {
        let schema = test_schema();
        let keys = infer_sort_keys(&schema);
        assert_eq!(keys, vec!["ts".to_string()]);
    }

    #[test]
    fn l0_file_tracking() {
        let cat = SagaCatalog::new();
        cat.register_subject_local(
            "events",
            test_schema(),
            vec!["ts".into()],
            PartitionGranularity::Day,
        )
        .unwrap();

        let meta = L0FileMeta {
            path: "/data/events/l0/part-001.parquet".into(),
            topic: "events".into(),
            partition_key: "dt=2026-03-06".into(),
            row_count: 100,
            file_size: 1024,
            min_timestamp: 0,
            max_timestamp: 100,
            min_sequence_id: 1,
            max_sequence_id: 10,
            created_at: chrono::Utc::now(),
        };
        cat.add_l0_file(meta);

        assert_eq!(cat.all_l0_files("events").len(), 1);
        assert_eq!(cat.l0_files("events", "dt=2026-03-06").len(), 1);
        assert_eq!(cat.l0_files("events", "dt=2026-03-07").len(), 0);
    }
}
