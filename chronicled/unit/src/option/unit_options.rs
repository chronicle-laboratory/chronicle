use serde::Deserialize;
use std::net::SocketAddr;

use super::auto_config::AutoConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IoMode {
    Basic,
    Advanced,
    Mmap,
}

impl Default for IoMode {
    fn default() -> Self {
        IoMode::Advanced
    }
}

#[derive(Debug, Deserialize)]
pub struct StorageOptions {
    #[serde(default = "default_storage_dir")]
    pub dir: String,
}

impl Default for StorageOptions {
    fn default() -> Self {
        Self {
            dir: default_storage_dir(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct WalOptions {
    #[serde(default = "default_wal_dir")]
    pub dir: String,
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            dir: default_wal_dir(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerOptions {
    #[serde(default = "default_server_address")]
    pub bind_address: SocketAddr,
    /// Address to register in the catalog for other nodes to connect to.
    /// Defaults to bind_address. Set this in Kubernetes to the pod's DNS name.
    #[serde(default)]
    pub advertise_address: Option<String>,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            bind_address: default_server_address(),
            advertise_address: None,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct LogOptions {
    #[serde(default = "default_log_level")]
    pub level: String,
}

impl Default for LogOptions {
    fn default() -> Self {
        Self {
            level: default_log_level(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CompactionOptions {
    /// Polling interval in ms. None = auto (5000ms).
    pub interval_ms: Option<u64>,
    /// Write cache capacity in MB. None = auto (10% of system memory).
    pub write_cache_capacity_mb: Option<usize>,
    /// L1 segment count before L2 merge. None = auto (4).
    pub l1_compaction_trigger: Option<usize>,
    /// L2 segment count before L3 split. None = auto (4).
    pub l2_compaction_trigger: Option<usize>,
    #[serde(default)]
    pub offload: Option<OffloadOptions>,
}

impl Default for CompactionOptions {
    fn default() -> Self {
        Self {
            interval_ms: None,
            write_cache_capacity_mb: None,
            l1_compaction_trigger: None,
            l2_compaction_trigger: None,
            offload: None,
        }
    }
}

/// Resolved compaction options with all values filled in.
#[derive(Debug, Clone)]
pub struct ResolvedCompactionOptions {
    pub interval_ms: u64,
    pub write_cache_capacity_mb: usize,
    pub l1_compaction_trigger: usize,
    pub l2_compaction_trigger: usize,
    pub offload: Option<OffloadOptions>,
}

impl CompactionOptions {
    pub fn resolve(&self, auto: &AutoConfig) -> ResolvedCompactionOptions {
        ResolvedCompactionOptions {
            interval_ms: self.interval_ms.unwrap_or(auto.compaction_interval_ms),
            write_cache_capacity_mb: self
                .write_cache_capacity_mb
                .unwrap_or(auto.write_cache_capacity_mb),
            l1_compaction_trigger: self.l1_compaction_trigger.unwrap_or(auto.l1_compaction_trigger),
            l2_compaction_trigger: self.l2_compaction_trigger.unwrap_or(auto.l2_compaction_trigger),
            offload: self.offload.clone(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct OffloadOptions {
    pub bucket: String,
    #[serde(default = "default_offload_prefix")]
    pub prefix: String,
    pub endpoint: Option<String>,
    pub region: Option<String>,
}

/// RocksDB index tuning. None = auto from system environment.
#[derive(Debug, Deserialize)]
pub struct IndexOptions {
    /// Block cache size in MB. None = auto (25% of system memory).
    pub block_cache_mb: Option<usize>,
    /// Memtable (write buffer) size in MB. None = auto (5% of system memory).
    pub write_buffer_mb: Option<usize>,
    /// Number of LSM levels. None = auto (4).
    pub num_levels: Option<i32>,
    /// SST target file size in MB. None = auto (4-16MB based on memory).
    pub target_file_size_mb: Option<u64>,
}

impl Default for IndexOptions {
    fn default() -> Self {
        Self {
            block_cache_mb: None,
            write_buffer_mb: None,
            num_levels: None,
            target_file_size_mb: None,
        }
    }
}

/// Resolved index options with all values filled in.
#[derive(Debug, Clone)]
pub struct ResolvedIndexOptions {
    pub block_cache_bytes: usize,
    pub write_buffer_size: usize,
    pub num_levels: i32,
    pub target_file_size_base: u64,
    pub max_bytes_for_level_base: u64,
}

impl IndexOptions {
    pub fn resolve(&self, auto: &AutoConfig) -> ResolvedIndexOptions {
        let block_cache_bytes = self
            .block_cache_mb
            .map(|mb| mb * 1024 * 1024)
            .unwrap_or(auto.block_cache_bytes);
        let write_buffer_size = self
            .write_buffer_mb
            .map(|mb| mb * 1024 * 1024)
            .unwrap_or(auto.write_buffer_size);
        let target_file_size_base = self
            .target_file_size_mb
            .map(|mb| mb * 1024 * 1024)
            .unwrap_or(auto.target_file_size_base);
        let num_levels = self.num_levels.unwrap_or(auto.num_levels);
        let max_bytes_for_level_base = target_file_size_base * 4;

        ResolvedIndexOptions {
            block_cache_bytes,
            write_buffer_size,
            num_levels,
            target_file_size_base,
            max_bytes_for_level_base,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct SegmentOptions {
    #[serde(default = "default_segments_dir")]
    pub dir: String,
    /// Segment size in MB (mmap initial capacity, WAL max size). None = auto.
    pub segment_size_mb: Option<u64>,
}

impl Default for SegmentOptions {
    fn default() -> Self {
        Self {
            dir: default_segments_dir(),
            segment_size_mb: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedSegmentOptions {
    pub dir: String,
    pub segment_size: u64,
}

impl SegmentOptions {
    pub fn resolve(&self, auto: &AutoConfig) -> ResolvedSegmentOptions {
        ResolvedSegmentOptions {
            dir: self.dir.clone(),
            segment_size: self
                .segment_size_mb
                .map(|mb| mb * 1024 * 1024)
                .unwrap_or(auto.segment_size),
        }
    }
}

/// Retention policy for automatic data eviction.
#[derive(Debug, Deserialize, Clone)]
pub struct RetentionOptions {
    /// Maximum age of events in hours. Events older than this are eligible for deletion.
    /// None = no TTL-based retention (keep forever).
    #[serde(default)]
    pub ttl_hours: Option<u64>,
    /// How often the retention manager runs, in seconds.
    #[serde(default = "default_retention_interval_secs")]
    pub interval_secs: u64,
}

impl Default for RetentionOptions {
    fn default() -> Self {
        Self {
            ttl_hours: None,
            interval_secs: default_retention_interval_secs(),
        }
    }
}

fn default_retention_interval_secs() -> u64 {
    3600 // 1 hour
}

#[derive(Debug, Deserialize)]
pub struct UnitOptions {
    #[serde(default)]
    pub wal: WalOptions,
    #[serde(default)]
    pub storage: StorageOptions,
    #[serde(default)]
    pub server: ServerOptions,
    #[serde(default)]
    pub log: LogOptions,
    #[serde(default)]
    pub catalog: catalog::CatalogOptions,
    #[serde(default)]
    pub compaction: CompactionOptions,
    #[serde(default)]
    pub segments: SegmentOptions,
    #[serde(default)]
    pub io_mode: IoMode,
    #[serde(default)]
    pub index: IndexOptions,
    #[serde(default)]
    pub retention: RetentionOptions,
}

impl Default for UnitOptions {
    fn default() -> Self {
        Self {
            wal: WalOptions::default(),
            storage: StorageOptions::default(),
            server: ServerOptions::default(),
            log: LogOptions::default(),
            catalog: catalog::CatalogOptions::default(),
            compaction: CompactionOptions::default(),
            segments: SegmentOptions::default(),
            io_mode: IoMode::default(),
            index: IndexOptions::default(),
            retention: RetentionOptions::default(),
        }
    }
}

fn default_server_address() -> SocketAddr {
    "127.0.0.1:7070".parse().unwrap()
}

fn default_log_level() -> String {
    String::from("info")
}

fn default_storage_dir() -> String {
    let mut path = std::env::temp_dir();
    path.push("chronicle");
    path.push("storage");
    path.to_string_lossy().to_string()
}

fn default_wal_dir() -> String {
    let mut path = std::env::temp_dir();
    path.push("chronicle");
    path.push("wal");
    path.to_string_lossy().to_string()
}

fn default_offload_prefix() -> String {
    "chronicle/segments".to_string()
}

fn default_segments_dir() -> String {
    let mut path = std::env::temp_dir();
    path.push("chronicle");
    path.push("segments");
    path.to_string_lossy().to_string()
}
