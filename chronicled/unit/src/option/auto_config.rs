use sysinfo::System;
use tracing::info;

use crate::option::unit_options::IoMode;

/// Detected system environment.
#[derive(Debug, Clone)]
pub struct SystemEnv {
    pub total_memory_bytes: u64,
    pub cpu_cores: usize,
}

impl SystemEnv {
    pub fn detect() -> Self {
        let sys = System::new_all();
        let env = Self {
            total_memory_bytes: sys.total_memory(),
            cpu_cores: num_cpus::get(),
        };
        info!(
            total_memory_mb = env.total_memory_bytes / (1024 * 1024),
            cpu_cores = env.cpu_cores,
            "detected system environment"
        );
        env
    }

    #[cfg(test)]
    pub fn with(memory_bytes: u64, cores: usize) -> Self {
        Self {
            total_memory_bytes: memory_bytes,
            cpu_cores: cores,
        }
    }
}

/// Auto-tuned configuration derived from system environment.
///
/// Memory budget is split roughly:
///   - 25% block cache (RocksDB index reads)
///   - 10% write cache (in-memory write buffer)
///   - 5%  RocksDB memtables
///   - 60% OS page cache, mmap segments, runtime
#[derive(Debug, Clone)]
pub struct AutoConfig {
    /// RocksDB block cache size in bytes.
    pub block_cache_bytes: usize,
    /// RocksDB write buffer (memtable) size in bytes.
    pub write_buffer_size: usize,
    /// Write cache capacity in MB.
    pub write_cache_capacity_mb: usize,
    /// Number of RocksDB LSM levels.
    pub num_levels: i32,
    /// SST target file size in bytes.
    pub target_file_size_base: u64,
    /// Max bytes for level base in bytes.
    pub max_bytes_for_level_base: u64,
    /// Compaction interval in ms.
    pub compaction_interval_ms: u64,
    /// L1 compaction trigger (segment count).
    pub l1_compaction_trigger: usize,
    /// L2 compaction trigger (segment count).
    pub l2_compaction_trigger: usize,
    /// Segment size in bytes (used for both mmap blob segments and WAL segments).
    pub segment_size: u64,
}

impl AutoConfig {
    pub fn from_env_with_io(env: &SystemEnv, io_mode: IoMode) -> Self {
        let total_mb = (env.total_memory_bytes / (1024 * 1024)) as usize;

        // Block cache: 25% of total memory, min 64MB, max 4GB.
        let block_cache_mb = (total_mb / 4).clamp(64, 4096);

        // Write cache: 10% of total memory, min 32MB, max 1GB
        let write_cache_mb = (total_mb / 10).clamp(32, 1024);

        // RocksDB memtable: 5% of total memory, min 4MB, max 256MB
        let write_buffer_mb = (total_mb / 20).clamp(4, 256);

        // SST target: scale with memory. Larger memory → larger SSTs → fewer files.
        let target_file_size_base = if total_mb >= 8192 {
            16 * 1024 * 1024 // 16MB
        } else if total_mb >= 2048 {
            8 * 1024 * 1024 // 8MB
        } else {
            4 * 1024 * 1024 // 4MB
        };

        let max_bytes_for_level_base = target_file_size_base * 4;

        // Segment size depends on I/O mode.
        // Mmap: larger segments — backed by OS page cache, cheap to map.
        // Basic/Advanced: smaller segments — buffered/direct I/O, less page cache pressure.
        let segment_mb = match io_mode {
            IoMode::Mmap => (total_mb / 32).clamp(64, 256),
            _ => (total_mb / 128).clamp(16, 64),
        };

        let config = Self {
            block_cache_bytes: block_cache_mb * 1024 * 1024,
            write_buffer_size: write_buffer_mb * 1024 * 1024,
            write_cache_capacity_mb: write_cache_mb,
            num_levels: 4,
            target_file_size_base: target_file_size_base as u64,
            max_bytes_for_level_base: max_bytes_for_level_base as u64,
            compaction_interval_ms: 5000,
            l1_compaction_trigger: 4,
            l2_compaction_trigger: 4,
            segment_size: (segment_mb as u64) * 1024 * 1024,
        };

        info!(
            block_cache_mb = block_cache_mb,
            write_cache_mb = write_cache_mb,
            write_buffer_mb = write_buffer_mb,
            target_file_size_mb = target_file_size_base / (1024 * 1024),
            segment_mb = segment_mb,
            "auto-configured from system environment"
        );

        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::option::unit_options::IoMode;

    #[test]
    fn test_auto_config_small_machine() {
        // 2GB, 2 cores, mmap → 2048MB total
        let env = SystemEnv::with(2 * 1024 * 1024 * 1024, 2);
        let config = AutoConfig::from_env_with_io(&env, IoMode::Mmap);

        assert_eq!(config.block_cache_bytes, 512 * 1024 * 1024); // 25% = 512MB
        assert_eq!(config.write_cache_capacity_mb, 204); // 10%
        assert_eq!(config.write_buffer_size, 102 * 1024 * 1024); // 5%
        assert_eq!(config.target_file_size_base, 8 * 1024 * 1024);
        assert_eq!(config.segment_size, 64 * 1024 * 1024); // mmap: 2048/32 = 64MB
    }

    #[test]
    fn test_auto_config_large_machine_basic() {
        // 64GB, 16 cores, basic I/O
        let env = SystemEnv::with(64 * 1024 * 1024 * 1024, 16);
        let config = AutoConfig::from_env_with_io(&env, IoMode::Basic);

        assert_eq!(config.block_cache_bytes, 4096 * 1024 * 1024);
        assert_eq!(config.segment_size, 64 * 1024 * 1024); // basic: capped at 64MB
    }

    #[test]
    fn test_auto_config_large_machine_mmap() {
        // 64GB, 16 cores, mmap I/O
        let env = SystemEnv::with(64 * 1024 * 1024 * 1024, 16);
        let config = AutoConfig::from_env_with_io(&env, IoMode::Mmap);

        assert_eq!(config.block_cache_bytes, 4096 * 1024 * 1024);
        assert_eq!(config.segment_size, 256 * 1024 * 1024); // mmap: capped at 256MB
    }

    #[test]
    fn test_auto_config_tiny_machine() {
        // 512MB, 1 core, mmap
        let env = SystemEnv::with(512 * 1024 * 1024, 1);
        let config = AutoConfig::from_env_with_io(&env, IoMode::Mmap);

        assert_eq!(config.block_cache_bytes, 128 * 1024 * 1024);
        assert_eq!(config.write_cache_capacity_mb, 51);
        assert_eq!(config.write_buffer_size, 25 * 1024 * 1024);
        assert_eq!(config.target_file_size_base, 4 * 1024 * 1024);
        assert_eq!(config.segment_size, 64 * 1024 * 1024); // mmap: min 64MB
    }
}
