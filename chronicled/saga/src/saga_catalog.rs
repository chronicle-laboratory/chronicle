use crate::error::{Result, SagaError};
use crate::types::{L0FileMeta, L1FileMeta, TopicConfig};
use arrow::datatypes::SchemaRef;
use parking_lot::RwLock;
use std::collections::HashMap;

struct TopicState {
    config: TopicConfig,
    schema: SchemaRef,
    l0_files: Vec<L0FileMeta>,
    l1_files: Vec<L1FileMeta>,
}

/// Manages topic metadata and file index for Saga.
pub struct SagaCatalog {
    topics: RwLock<HashMap<String, TopicState>>,
}

impl SagaCatalog {
    pub fn new() -> Self {
        Self {
            topics: RwLock::new(HashMap::new()),
        }
    }

    pub fn register_topic(&self, config: TopicConfig) -> Result<()> {
        let schema = config.to_arrow_schema();
        let name = config.name.clone();
        let mut topics = self.topics.write();
        if topics.contains_key(&name) {
            return Err(SagaError::Internal(format!("topic '{}' already exists", name)));
        }
        topics.insert(name, TopicState {
            config,
            schema,
            l0_files: Vec::new(),
            l1_files: Vec::new(),
        });
        Ok(())
    }

    pub fn schema(&self, topic: &str) -> Result<SchemaRef> {
        let topics = self.topics.read();
        topics
            .get(topic)
            .map(|t| t.schema.clone())
            .ok_or_else(|| SagaError::TopicNotFound(topic.into()))
    }

    pub fn topic_config(&self, topic: &str) -> Result<TopicConfig> {
        let topics = self.topics.read();
        topics
            .get(topic)
            .map(|t| t.config.clone())
            .ok_or_else(|| SagaError::TopicNotFound(topic.into()))
    }

    pub fn add_l0_file(&self, meta: L0FileMeta) {
        let mut topics = self.topics.write();
        if let Some(state) = topics.get_mut(&meta.topic) {
            state.l0_files.push(meta);
        }
    }

    pub fn l0_files(&self, topic: &str, partition: &str) -> Vec<L0FileMeta> {
        let topics = self.topics.read();
        topics
            .get(topic)
            .map(|t| {
                t.l0_files
                    .iter()
                    .filter(|f| f.partition_key == partition)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn all_l0_files(&self, topic: &str) -> Vec<L0FileMeta> {
        let topics = self.topics.read();
        topics
            .get(topic)
            .map(|t| t.l0_files.clone())
            .unwrap_or_default()
    }

    /// After merge: remove merged L0 files, add new L1 files.
    pub fn complete_merge(
        &self,
        topic: &str,
        merged_l0_paths: &[String],
        new_l1: Vec<L1FileMeta>,
    ) {
        let mut topics = self.topics.write();
        if let Some(state) = topics.get_mut(topic) {
            state.l0_files.retain(|f| !merged_l0_paths.contains(&f.path));
            state.l1_files.extend(new_l1);
        }
    }

    pub fn l1_files(&self, topic: &str) -> Vec<L1FileMeta> {
        let topics = self.topics.read();
        topics
            .get(topic)
            .map(|t| t.l1_files.clone())
            .unwrap_or_default()
    }

    pub fn list_topics(&self) -> Vec<String> {
        self.topics.read().keys().cloned().collect()
    }

    /// Get all unique partitions for a topic from L0 files.
    pub fn l0_partitions(&self, topic: &str) -> Vec<String> {
        let topics = self.topics.read();
        topics
            .get(topic)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{FieldDef, FieldType, PartitionGranularity};

    fn test_topic() -> TopicConfig {
        TopicConfig {
            name: "events".into(),
            schema: vec![
                FieldDef { name: "ts".into(), data_type: FieldType::TimestampMillis, nullable: false },
                FieldDef { name: "val".into(), data_type: FieldType::Int64, nullable: true },
            ],
            sort_keys: vec!["ts".into()],
            partition_granularity: PartitionGranularity::Day,
        }
    }

    #[test]
    fn register_and_query_topic() {
        let cat = SagaCatalog::new();
        cat.register_topic(test_topic()).unwrap();
        assert_eq!(cat.list_topics(), vec!["events".to_string()]);
        assert!(cat.schema("events").is_ok());
        assert!(cat.schema("nonexistent").is_err());
    }

    #[test]
    fn l0_file_tracking() {
        let cat = SagaCatalog::new();
        cat.register_topic(test_topic()).unwrap();

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
