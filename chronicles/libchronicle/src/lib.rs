use crate::error::ChronicleError;

pub mod chronicle;
pub mod conn;
pub mod cursor;
pub mod error;
pub mod observability;
pub mod timeline;

#[derive(Debug, Clone)]
pub struct Event {
    pub payload: Vec<u8>,
    pub key: Option<Vec<u8>>,
    pub schema_id: Option<i64>,
    pub txn_id: Option<i64>,
}

impl Event {
    pub fn new(payload: Vec<u8>) -> Self {
        Self {
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

#[derive(Debug, Clone)]
pub struct FetchedEvent {
    pub offset: i64,
    pub timestamp: i64,
    pub payload: Vec<u8>,
    pub key: Option<Vec<u8>>,
    pub schema_id: Option<i64>,
}

#[async_trait::async_trait]
pub trait Writer {
    async fn record(&self, event: Event) -> Result<Offset, ChronicleError>;
}

#[async_trait::async_trait]
pub trait Cursor {
    async fn fetch(&mut self) -> Result<Option<FetchedEvent>, ChronicleError>;
    fn seek(&mut self, offset: i64);
    fn position(&self) -> i64;
}
