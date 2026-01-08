use thiserror::Error;

#[derive(Error, Debug)]
pub enum CatalogError {
    #[error("Chronicle not found")]
    ChronicleNotFound,
    #[error("Unit not found")]
    UnitNotFound,
    #[error("Verse not found: {verse}")]
    VerseNotFound { verse: String },
    #[error("Timeline not found: {id} in verse {verse}")]
    TimelineNotFound { id: u64, verse: String },
    #[error("Storage error: {0}")]
    Storage(String),
}

