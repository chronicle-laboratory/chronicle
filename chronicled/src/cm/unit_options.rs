use serde::Deserialize;
use std::net::SocketAddr;

#[derive(Debug, Deserialize)]
pub struct Meta {
    pub id: u64,
    #[serde(default = "default_meta_address")]
    pub bind_address: SocketAddr,
}

#[derive(Debug, Deserialize)]
pub struct Storage {
    #[serde(default = "default_temp_dir")]
    pub dir: String,
}

#[derive(Debug, Deserialize)]
pub struct Wal {
    #[serde(default = "default_temp_dir")]
    pub dir: String,
}

#[derive(Debug, Deserialize)]
pub struct Server {
    #[serde(default = "default_server_address")]
    pub bind_address: SocketAddr,
}

#[derive(Debug, Deserialize)]
pub struct Log {
    #[serde(default = "default_log_level")]
    pub level: String,
}

#[derive(Debug, Deserialize)]
pub struct UnitOptions {
    pub meta: Meta,
    pub wal: Wal,
    pub storage: Storage,
    pub server: Server,
    pub log: Log,
}

fn default_meta_address() -> SocketAddr {
    "127.0.0.1:7071".parse().unwrap()
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
