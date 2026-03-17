use crate::error::ChronicleError;

pub mod chronicle;
pub mod conn;
pub mod cursor;
pub mod ensemble;
pub mod error;
pub mod observability;
pub mod replicator;
pub mod timeline;

#[derive(Debug, Clone)]
pub struct Event {
    pub offset: Option<i64>,
    pub timestamp: Option<i64>,
    pub payload: Vec<u8>,
    pub key: Option<Vec<u8>>,
    pub schema_id: Option<i64>,
    pub txn_id: Option<i64>,
}

impl Event {
    pub fn new(payload: Vec<u8>) -> Self {
        Self {
            offset: None,
            timestamp: None,
            payload,
            key: None,
            schema_id: None,
            txn_id: None,
        }
    }

    pub fn with_key(mut self, key: Vec<u8>) -> Self {
        self.key = Some(key);
        self
    }

    pub fn with_schema_id(mut self, schema_id: i64) -> Self {
        self.schema_id = Some(schema_id);
        self
    }

    pub fn with_txn_id(mut self, txn_id: i64) -> Self {
        self.txn_id = Some(txn_id);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Offset(pub i64);

pub use cursor::EventStream;

#[derive(Debug, Clone)]
pub enum StartPosition {
    Earliest,
    Latest,
    Offset(i64),
    Index { name: String, value: String },
}

#[derive(Debug, Clone)]
pub struct FetchOptions {
    pub(crate) start: StartPosition,
    pub(crate) limit: Option<usize>,
    pub(crate) timeout: Option<std::time::Duration>,
}

impl FetchOptions {
    pub fn earliest() -> Self {
        Self {
            start: StartPosition::Earliest,
            limit: None,
            timeout: None,
        }
    }

    pub fn latest() -> Self {
        Self {
            start: StartPosition::Latest,
            limit: None,
            timeout: None,
        }
    }

    pub fn offset(offset: i64) -> Self {
        Self {
            start: StartPosition::Offset(offset),
            limit: None,
            timeout: None,
        }
    }

    pub fn index(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            start: StartPosition::Index {
                name: name.into(),
                value: value.into(),
            },
            limit: None,
            timeout: None,
        }
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

#[async_trait::async_trait]
pub trait Writer {
    async fn record(&self, event: Event) -> Result<Offset, ChronicleError>;
}
