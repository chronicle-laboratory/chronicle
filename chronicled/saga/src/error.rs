#[derive(Debug, thiserror::Error)]
pub enum SagaError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::Status),
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),
    #[error("Config error: {0}")]
    Config(String),
    #[error("Decode error: {0}")]
    Decode(String),
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("Topic not found: {0}")]
    TopicNotFound(String),
}

pub type Result<T> = std::result::Result<T, SagaError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display() {
        let e = SagaError::Config("bad value".into());
        assert!(e.to_string().contains("bad value"));
    }
}
