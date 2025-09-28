use crate::error::ChronicleError;
use futures_util::Stream;

pub mod catalog;
pub mod chronicle;
pub mod ensemble;
pub mod error;
pub mod parallel_timeline;
pub mod parallel_timeline_reader;
pub mod reconciliation;
pub mod unit_client;

#[derive(Debug, Clone)]
pub struct Offset {
    pub timeline_id: i64,
    pub offset: i64,
}

pub trait Acknowledgeable {
    async fn acknowledge(&self, offset: Offset) -> Result<(), ChronicleError>;
    async fn acknowledge_to(&self, offset: Offset) -> Result<(), ChronicleError>;
}

pub trait Appendable {
    async fn append(&self, payload: Vec<u8>) -> Result<Offset, ChronicleError>;
}

pub trait Fetchable {
    async fn fetch(
        &self,
    ) -> Result<impl Stream<Item = Result<(Offset, Vec<u8>), ChronicleError>>, ChronicleError>;
    async fn fetch_next(&self) -> Result<(Offset, Vec<u8>), ChronicleError>;
}

pub trait Seekable {
    async fn seek(&self, offset: i64) -> Result<(), ChronicleError>;
}
