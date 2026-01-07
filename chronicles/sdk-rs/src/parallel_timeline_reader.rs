use crate::errors::ChronicleError;
use crate::{Acknowledgeable, Fetchable, Offset, Seekable};
use async_stream::stream;
use futures_util::Stream;

pub enum ReaderType {
    Offset,
    SecondaryOffset,
}

pub struct ParallelTimelineReaderOptions {
    pub cursor_name: Option<String>,
    pub reader_type: ReaderType,
    pub secondary_offset_name: Option<String>,
}

pub struct ParallelTimelineReader {}

impl ParallelTimelineReader {
    pub fn new(name: String, options: ParallelTimelineReaderOptions) -> Self {
        todo!()
    }
}

impl Fetchable for ParallelTimelineReader {
    async fn fetch(
        &self,
    ) -> Result<impl Stream<Item = Result<(Offset, Vec<u8>), ChronicleError>>, ChronicleError> {
        Ok(stream! {
            yield Ok((Offset{ timeline_id: 0,offset: 0,}, vec![]))
        })
    }

    async fn fetch_next(&self) -> Result<(Offset, Vec<u8>), ChronicleError> {
        todo!()
    }
}

impl Acknowledgeable for ParallelTimelineReader {
    async fn acknowledge(&self, offset: Offset) -> Result<(), ChronicleError> {
        todo!()
    }

    async fn acknowledge_to(&self, offset: Offset) -> Result<(), ChronicleError> {
        todo!()
    }
}

impl Seekable for ParallelTimelineReader {
    async fn seek(&self, offset: i64) -> Result<(), ChronicleError> {
        todo!()
    }
}
