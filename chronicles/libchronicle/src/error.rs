use thiserror::Error;

#[derive(Error, Debug)]
pub enum ChronicleError {
    #[error("Unsupported operation: {0}")]
    Unsupported(String),
    #[error("Transport error: {0}")]
    Transport(String),
    #[error("Fenced at term {0}")]
    Fenced(i64),
    #[error("Timeline not found: {0}")]
    TimelineNotFound(String),
    #[error("Unit unavailable: {0}")]
    UnitUnavailable(String),
    #[error("Catalog version conflict: expected {expected}, got {actual}")]
    CatalogVersionConflict { expected: i64, actual: i64 },
    #[error("Timeout: {0}")]
    Timeout(String),
    #[error("Internal error: {0}")]
    Internal(String),
}
