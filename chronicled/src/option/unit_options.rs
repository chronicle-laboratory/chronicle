use serde::Deserialize;
use std::net::SocketAddr;

#[derive(Debug, Deserialize)]
pub struct StorageOptions {
    #[serde(default = "default_temp_dir")]
    pub dir: String,
}

#[derive(Debug, Deserialize)]
pub struct WalOptions {
    #[serde(default = "default_temp_dir")]
    pub dir: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerOptions {
    #[serde(default = "default_server_address")]
    pub bind_address: SocketAddr,
}

#[derive(Debug, Deserialize)]
pub struct LogOptions {
    #[serde(default = "default_log_level")]
    pub level: String,
}

#[derive(Debug, Deserialize)]
pub struct UnitOptions {
    pub wal: WalOptions,
    pub storage: StorageOptions,
    pub server: ServerOptions,
    pub log: LogOptions,
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
