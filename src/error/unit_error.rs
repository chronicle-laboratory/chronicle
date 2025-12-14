use thiserror::Error;

#[derive(Error, Debug)]
pub enum UnitError {
    #[error("Metadata node aware error: {0}")]
    MetadataNodeAware(String),

    #[error("Transport error: {0}")]
    Transport(String),

    #[error("Codec error: {0}")]
    Codec(String),

    #[error("Storage error: {0}")]
    Storage(String),
}
