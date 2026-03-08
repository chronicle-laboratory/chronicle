use serde::{Deserialize, Serialize};

/// Lexicon 服务配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LexiconConfig {
    /// Oxia 连接地址
    #[serde(default = "default_oxia_endpoint")]
    pub oxia_endpoint: String,
    /// gRPC 端口
    #[serde(default = "default_grpc_port")]
    pub grpc_port: u16,
    /// HTTP 端口
    #[serde(default = "default_http_port")]
    pub http_port: u16,
    /// Oxia key 前缀
    #[serde(default = "default_key_prefix")]
    pub key_prefix: String,
    /// 是否使用本地文件存储（开发模式）
    #[serde(default = "default_true")]
    pub use_local_store: bool,
    /// 本地存储目录
    #[serde(default = "default_local_store_dir")]
    pub local_store_dir: String,
}

impl Default for LexiconConfig {
    fn default() -> Self {
        Self {
            oxia_endpoint: default_oxia_endpoint(),
            grpc_port: default_grpc_port(),
            http_port: default_http_port(),
            key_prefix: default_key_prefix(),
            use_local_store: true,
            local_store_dir: default_local_store_dir(),
        }
    }
}

fn default_oxia_endpoint() -> String {
    "localhost:6648".to_string()
}
fn default_grpc_port() -> u16 {
    50060
}
fn default_http_port() -> u16 {
    8080
}
fn default_key_prefix() -> String {
    "/chronicle/lexicon".to_string()
}
fn default_true() -> bool {
    true
}
fn default_local_store_dir() -> String {
    "/tmp/lexicon".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = LexiconConfig::default();
        assert_eq!(config.grpc_port, 50060);
        assert_eq!(config.http_port, 8080);
        assert!(config.use_local_store);
    }

    #[test]
    fn test_deserialize_config() {
        let json = r#"{"grpc_port": 9090, "http_port": 9091}"#;
        let config: LexiconConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.grpc_port, 9090);
        assert_eq!(config.http_port, 9091);
        assert!(config.use_local_store); // default
    }
}
