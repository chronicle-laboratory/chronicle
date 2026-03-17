use sysinfo::System;
use tracing::info;

use crate::option::unit_options::IoMode;

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

#[derive(Debug, Clone)]
pub struct AutoConfig {
    pub block_cache_bytes: usize,
    pub write_buffer_size: usize,
    pub write_cache_capacity_mb: usize,
    pub num_levels: i32,
    pub target_file_size_base: u64,
    pub max_bytes_for_level_base: u64,
    pub compaction_interval_ms: u64,
    pub l1_compaction_trigger: usize,
    pub l2_compaction_trigger: usize,
    pub segment_size: u64,
}

impl AutoConfig {
    pub fn from_env_with_io(env: &SystemEnv, io_mode: IoMode) -> Self {
        let total_mb = (env.total_memory_bytes / (1024 * 1024)) as usize;

        let block_cache_mb = (total_mb / 4).clamp(64, 4096);

        let write_cache_mb = (total_mb / 10).clamp(32, 1024);

        let write_buffer_mb = (total_mb / 20).clamp(4, 256);

        let target_file_size_base = if total_mb >= 8192 {
            16 * 1024 * 1024
        } else if total_mb >= 2048 {
            8 * 1024 * 1024
        } else {
            4 * 1024 * 1024
        };

        let max_bytes_for_level_base = target_file_size_base * 4;

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
        let env = SystemEnv::with(2 * 1024 * 1024 * 1024, 2);
        let config = AutoConfig::from_env_with_io(&env, IoMode::Mmap);

        assert_eq!(config.block_cache_bytes, 512 * 1024 * 1024);
        assert_eq!(config.write_cache_capacity_mb, 204);
        assert_eq!(config.write_buffer_size, 102 * 1024 * 1024);
        assert_eq!(config.target_file_size_base, 8 * 1024 * 1024);
        assert_eq!(config.segment_size, 64 * 1024 * 1024);
    }

    #[test]
    fn test_auto_config_large_machine_basic() {
        let env = SystemEnv::with(64 * 1024 * 1024 * 1024, 16);
        let config = AutoConfig::from_env_with_io(&env, IoMode::Basic);

        assert_eq!(config.block_cache_bytes, 4096 * 1024 * 1024);
        assert_eq!(config.segment_size, 64 * 1024 * 1024);
    }

    #[test]
    fn test_auto_config_large_machine_mmap() {
        let env = SystemEnv::with(64 * 1024 * 1024 * 1024, 16);
        let config = AutoConfig::from_env_with_io(&env, IoMode::Mmap);

        assert_eq!(config.block_cache_bytes, 4096 * 1024 * 1024);
        assert_eq!(config.segment_size, 256 * 1024 * 1024);
    }

    #[test]
    fn test_auto_config_tiny_machine() {
        let env = SystemEnv::with(512 * 1024 * 1024, 1);
        let config = AutoConfig::from_env_with_io(&env, IoMode::Mmap);

        assert_eq!(config.block_cache_bytes, 128 * 1024 * 1024);
        assert_eq!(config.write_cache_capacity_mb, 51);
        assert_eq!(config.write_buffer_size, 25 * 1024 * 1024);
        assert_eq!(config.target_file_size_base, 4 * 1024 * 1024);
        assert_eq!(config.segment_size, 64 * 1024 * 1024);
    }
}
