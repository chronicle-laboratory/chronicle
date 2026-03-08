use thiserror::Error;

#[derive(Debug, Error)]
pub enum LexiconError {
    #[error("subject already exists: {0}")]
    SubjectAlreadyExists(String),

    #[error("subject not found: {0}")]
    SubjectNotFound(String),

    #[error("schema version not found: {subject} v{version}")]
    VersionNotFound { subject: String, version: u32 },

    #[error("wire format mismatch: subject uses {expected:?}, got {got:?}")]
    WireFormatMismatch {
        expected: String,
        got: String,
    },

    #[error("schema input does not match wire format {wire_format}: {detail}")]
    SchemaInputMismatch {
        wire_format: String,
        detail: String,
    },

    #[error("incompatible schema: {violations:?}")]
    IncompatibleSchema { violations: Vec<String> },

    #[error("invalid schema: {0}")]
    InvalidSchema(String),

    #[error("invalid basic type: {0}")]
    InvalidBasicType(String),

    #[error("store error: {0}")]
    Store(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<serde_json::Error> for LexiconError {
    fn from(e: serde_json::Error) -> Self {
        LexiconError::Serialization(e.to_string())
    }
}

impl From<LexiconError> for tonic::Status {
    fn from(e: LexiconError) -> Self {
        match &e {
            LexiconError::SubjectAlreadyExists(_) => tonic::Status::already_exists(e.to_string()),
            LexiconError::SubjectNotFound(_) | LexiconError::VersionNotFound { .. } => {
                tonic::Status::not_found(e.to_string())
            }
            LexiconError::WireFormatMismatch { .. }
            | LexiconError::SchemaInputMismatch { .. }
            | LexiconError::InvalidSchema(_)
            | LexiconError::InvalidBasicType(_)
            | LexiconError::IncompatibleSchema { .. } => {
                tonic::Status::invalid_argument(e.to_string())
            }
            _ => tonic::Status::internal(e.to_string()),
        }
    }
}

pub type Result<T> = std::result::Result<T, LexiconError>;
