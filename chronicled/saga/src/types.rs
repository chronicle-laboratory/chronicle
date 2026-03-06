use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Topic configuration specified when creating a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfig {
    pub name: String,
    pub schema: Vec<FieldDef>,
    /// Sort keys (default: ["timestamp"])
    #[serde(default = "default_sort_keys")]
    pub sort_keys: Vec<String>,
    /// Partition granularity (default: Day)
    #[serde(default)]
    pub partition_granularity: PartitionGranularity,
}

fn default_sort_keys() -> Vec<String> {
    vec!["timestamp".into()]
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDef {
    pub name: String,
    pub data_type: FieldType,
    #[serde(default)]
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldType {
    Int64,
    Float64,
    Utf8,
    TimestampMillis,
    Boolean,
    Binary,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum PartitionGranularity {
    Hour,
    #[default]
    Day,
    Month,
    None,
}

impl FieldDef {
    pub fn to_arrow_field(&self) -> Field {
        let dt = match self.data_type {
            FieldType::Int64 => DataType::Int64,
            FieldType::Float64 => DataType::Float64,
            FieldType::Utf8 => DataType::Utf8,
            FieldType::TimestampMillis => DataType::Timestamp(TimeUnit::Millisecond, None),
            FieldType::Boolean => DataType::Boolean,
            FieldType::Binary => DataType::Binary,
        };
        Field::new(&self.name, dt, self.nullable)
    }
}

impl TopicConfig {
    pub fn to_arrow_schema(&self) -> SchemaRef {
        let fields: Vec<Field> = self.schema.iter().map(|f| f.to_arrow_field()).collect();
        Arc::new(Schema::new(fields))
    }
}

/// L0 file metadata (local Parquet files).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L0FileMeta {
    pub path: String,
    pub topic: String,
    pub partition_key: String,
    pub row_count: usize,
    pub file_size: usize,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub min_sequence_id: u64,
    pub max_sequence_id: u64,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// L1 file metadata (remote / S3 Parquet files).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct L1FileMeta {
    pub s3_path: String,
    pub partition_key: String,
    pub row_count: usize,
    pub file_size: usize,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
}

/// Watermarks tracking ingestion progress.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Watermarks {
    /// Consumed into memory.
    pub ingested: u64,
    /// Flushed to local L0 Parquet.
    pub flushed: u64,
    /// Uploaded to S3 (persisted).
    pub persisted: u64,
}

/// Compute the partition key for a given timestamp.
pub fn partition_key(ts_millis: i64, granularity: &PartitionGranularity) -> String {
    let secs = ts_millis / 1000;
    let dt = chrono::DateTime::from_timestamp(secs, 0)
        .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
    match granularity {
        PartitionGranularity::Hour => dt.format("dt=%Y-%m-%d-%H").to_string(),
        PartitionGranularity::Day => dt.format("dt=%Y-%m-%d").to_string(),
        PartitionGranularity::Month => dt.format("dt=%Y-%m").to_string(),
        PartitionGranularity::None => "dt=all".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_def_to_arrow() {
        let f = FieldDef {
            name: "ts".into(),
            data_type: FieldType::TimestampMillis,
            nullable: false,
        };
        let af = f.to_arrow_field();
        assert_eq!(af.name(), "ts");
        assert!(!af.is_nullable());
    }

    #[test]
    fn topic_config_to_schema() {
        let cfg = TopicConfig {
            name: "events".into(),
            schema: vec![
                FieldDef { name: "timestamp".into(), data_type: FieldType::TimestampMillis, nullable: false },
                FieldDef { name: "value".into(), data_type: FieldType::Int64, nullable: true },
            ],
            sort_keys: vec!["timestamp".into()],
            partition_granularity: PartitionGranularity::Day,
        };
        let schema = cfg.to_arrow_schema();
        assert_eq!(schema.fields().len(), 2);
    }

    #[test]
    fn partition_key_day() {
        // 2026-03-06 00:00:00 UTC
        let ts = 1772611200000i64;
        let key = partition_key(ts, &PartitionGranularity::Day);
        assert!(key.starts_with("dt="));
    }

    #[test]
    fn partition_key_none() {
        let key = partition_key(0, &PartitionGranularity::None);
        assert_eq!(key, "dt=all");
    }
}
