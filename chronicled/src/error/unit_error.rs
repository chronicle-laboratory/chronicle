use chronicle_catalog::errors::CatalogError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum UnitError {
    #[error("Catalog operation failed: {0}")]
    Catalog(#[from] CatalogError),

    #[error("Resource is unavailable: {0}")]
    Unavailable(String),
    #[error("Transport error: {0}")]
    Transport(String),
    #[error("Codec error: {0}")]
    Codec(String),
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Task error: {0}")]
    TaskError(String),
    #[error("WAL error")]
    Wal,
}
