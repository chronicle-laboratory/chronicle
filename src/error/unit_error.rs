use thiserror::Error;

#[derive(Error, Debug)]
pub enum UnitError {
    #[error("Metadata node aware error: {0}")]
    MetadataNodeAware(String),

    #[error("Metadata partition error: {0}")]
    MetadataPartition(String),

    #[error("Metadata partition not leader error")]
    MetadataPartitionNotLeader(),

    #[error("Transport error: {0}")]
    Transport(String),
}
