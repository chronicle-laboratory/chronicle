use chronicle_catalog::errors::CatalogError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum UnitError {
    #[error("Catalog operation failed: {0}")]
    Catalog(#[from] CatalogError),
    #[error("Transport error: {0}")]
    Transport(String),
    #[error("Codec error: {0}")]
    Codec(String),
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Task error: {0}")]
    TaskError(String),
}
