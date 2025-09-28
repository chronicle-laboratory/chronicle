use thiserror::Error;

#[derive(Error, Debug)]
pub enum CatalogError {
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Version conflict: expected {expected}, got {actual}")]
    VersionConflict { expected: i64, actual: i64 },
    #[error("Transport error: {0}")]
    Transport(String),
    #[error("Internal error: {0}")]
    Internal(String),
}
