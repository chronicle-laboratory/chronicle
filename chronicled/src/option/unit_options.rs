use serde::Deserialize;
use std::net::SocketAddr;

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
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            bind_address: default_server_address(),
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
    #[serde(default = "default_compaction_interval_ms")]
    pub interval_ms: u64,
    #[serde(default = "default_write_cache_capacity_mb")]
    pub write_cache_capacity_mb: usize,
}

impl Default for CompactionOptions {
    fn default() -> Self {
        Self {
            interval_ms: default_compaction_interval_ms(),
            write_cache_capacity_mb: default_write_cache_capacity_mb(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct SegmentOptions {
    #[serde(default = "default_segments_dir")]
    pub dir: String,
}

impl Default for SegmentOptions {
    fn default() -> Self {
        Self {
            dir: default_segments_dir(),
        }
    }
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

fn default_compaction_interval_ms() -> u64 {
    5000
}

fn default_write_cache_capacity_mb() -> usize {
    64
}

fn default_segments_dir() -> String {
    let mut path = std::env::temp_dir();
    path.push("chronicle");
    path.push("segments");
    path.to_string_lossy().to_string()
}

