use crate::errors::ChronicleError;
use crate::errors::ChronicleError::Unsupported;
use crate::{Acknowledgeable, Fetchable, Offset, Seekable};
use async_stream::stream;
use futures_util::Stream;

#[derive(PartialEq, Debug)]
pub enum ReadType {
    Offset,
    SecondaryOffset,
}

pub struct ParallelTimelineReaderOptions {
    pub cursor_name: Option<String>,
    pub read_type: ReadType,
    pub secondary_offset_name: Option<String>,
}

pub struct ParallelTimelineReader {
    name: String,
    cursor_name: Option<String>,
    read_type: ReadType,
    secondary_offset_name: Option<String>,
}

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
        if self.read_type == ReadType::SecondaryOffset {
            return Err(Unsupported(format!(
                "unsupported cumulative acknowledge for read type {:?}",
                self.read_type
            )));
        }
        todo!()
    }
}

impl Seekable for ParallelTimelineReader {
    async fn seek(&self, offset: i64) -> Result<(), ChronicleError> {
        todo!()
    }
}
