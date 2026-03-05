use crate::error::ChronicleError;

pub mod chronicle;
pub mod client;
pub mod error;
pub mod cursor;
pub mod observability;
pub mod writer;

#[derive(Debug, Clone)]
pub struct Offset {
    pub timeline_id: i64,
    pub offset: i64,
}

#[async_trait::async_trait]
pub trait Writer {
    async fn record(&self, event: Vec<u8>) -> Result<Offset, ChronicleError>;
}

#[async_trait::async_trait]
pub trait Cursor {
    async fn fetch(&mut self) -> Result<Option<(Offset, Vec<u8>)>, ChronicleError>;
    fn seek(&mut self, offset: i64);
}
