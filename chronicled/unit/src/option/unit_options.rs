use serde::Deserialize;
use std::net::SocketAddr;

#[derive(Debug, Deserialize)]
pub struct StorageOptions {
    #[serde(default = "default_temp_dir")]
    pub dir: String,
}

impl Default for StorageOptions {
    fn default() -> Self {
        Self {
            dir: default_temp_dir(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct WalOptions {
    #[serde(default = "default_temp_dir")]
    pub dir: String,
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            dir: default_temp_dir(),
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
pub struct UnitOptions {
    #[serde(default)]
    pub wal: WalOptions,
    #[serde(default)]
    pub storage: StorageOptions,
    #[serde(default)]
    pub server: ServerOptions,
    #[serde(default)]
    pub log: LogOptions,
}

impl Default for UnitOptions {
    fn default() -> Self {
        Self {
            wal: WalOptions::default(),
            storage: StorageOptions::default(),
            server: ServerOptions::default(),
            log: LogOptions::default(),
        }
    }
}

fn default_server_address() -> SocketAddr {
    "127.0.0.1:7070".parse().unwrap()
}

fn default_log_level() -> String {
    String::from("info")
}

fn default_temp_dir() -> String {
    std::env::temp_dir().to_string_lossy().to_string()
}
