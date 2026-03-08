use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaConfig {
    /// WAL gRPC endpoint (e.g. "http://127.0.0.1:7070")
    #[serde(default = "default_wal_endpoint")]
    pub wal_endpoint: String,

    /// S3 storage root (e.g. "s3://bucket-name/saga")
    #[serde(default = "default_storage_root")]
    pub storage_root: String,

    /// Local L0 data directory
    #[serde(default = "default_local_data_dir")]
    pub local_data_dir: String,

    /// Watermark checkpoint file path
    #[serde(default = "default_checkpoint_path")]
    pub checkpoint_path: String,

    /// Memtable flush size threshold in bytes (default 128MB)
    #[serde(default = "default_flush_size")]
    pub flush_size_threshold: usize,

    /// Memtable flush age threshold in seconds (default 300)
    #[serde(default = "default_flush_age")]
    pub flush_age_threshold_secs: u64,

    /// Merger trigger: min L0 files per partition before merging (default 4)
    #[serde(default = "default_merge_threshold")]
    pub merge_file_threshold: usize,

    /// Merger target file size in bytes (default 256MB)
    #[serde(default = "default_merge_target")]
    pub merge_target_size: usize,

    /// Parquet row group size in bytes (default 128MB)
    #[serde(default = "default_row_group_size")]
    pub parquet_row_group_size: usize,

    /// WAL poll interval in milliseconds (default 1000)
    #[serde(default = "default_wal_poll")]
    pub wal_poll_interval_ms: u64,

    /// Merge check interval in seconds (default 60)
    #[serde(default = "default_merge_interval")]
    pub merge_check_interval_secs: u64,

    /// HTTP service port (default 3000)
    #[serde(default = "default_http_port")]
    pub http_port: u16,

    /// Lexicon (schema registry) gRPC endpoint
    #[serde(default = "default_lexicon_endpoint")]
    pub lexicon_endpoint: String,

    /// Use local filesystem instead of S3 (dev mode)
    #[serde(default)]
    pub use_local_fs: bool,
}

fn default_wal_endpoint() -> String { "http://127.0.0.1:7070".into() }
fn default_storage_root() -> String { "/tmp/saga/remote".into() }
fn default_local_data_dir() -> String { "/tmp/saga/data".into() }
fn default_checkpoint_path() -> String { "/tmp/saga/checkpoint.json".into() }
fn default_flush_size() -> usize { 128 * 1024 * 1024 }
fn default_flush_age() -> u64 { 300 }
fn default_merge_threshold() -> usize { 4 }
fn default_merge_target() -> usize { 256 * 1024 * 1024 }
fn default_row_group_size() -> usize { 128 * 1024 * 1024 }
fn default_wal_poll() -> u64 { 1000 }
fn default_merge_interval() -> u64 { 60 }
fn default_http_port() -> u16 { 3000 }
fn default_lexicon_endpoint() -> String { "http://chronicle-lexicon:50060".into() }

impl Default for SagaConfig {
    fn default() -> Self {
        Self {
            wal_endpoint: default_wal_endpoint(),
            storage_root: default_storage_root(),
            local_data_dir: default_local_data_dir(),
            checkpoint_path: default_checkpoint_path(),
            flush_size_threshold: default_flush_size(),
            flush_age_threshold_secs: default_flush_age(),
            merge_file_threshold: default_merge_threshold(),
            merge_target_size: default_merge_target(),
            parquet_row_group_size: default_row_group_size(),
            wal_poll_interval_ms: default_wal_poll(),
            merge_check_interval_secs: default_merge_interval(),
            http_port: default_http_port(),
            lexicon_endpoint: default_lexicon_endpoint(),
            use_local_fs: true,
        }
    }
}

impl SagaConfig {
    pub fn from_file(path: &str) -> crate::error::Result<Self> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| crate::error::SagaError::Config(format!("read config: {}", e)))?;
        serde_json::from_str(&contents)
            .map_err(|e| crate::error::SagaError::Config(format!("parse config: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let cfg = SagaConfig::default();
        assert_eq!(cfg.http_port, 3000);
        assert_eq!(cfg.flush_size_threshold, 128 * 1024 * 1024);
        assert!(cfg.use_local_fs);
    }

    #[test]
    fn deserialize_partial() {
        let json = r#"{"http_port": 4000}"#;
        let cfg: SagaConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.http_port, 4000);
        assert_eq!(cfg.flush_size_threshold, 128 * 1024 * 1024);
    }
}
