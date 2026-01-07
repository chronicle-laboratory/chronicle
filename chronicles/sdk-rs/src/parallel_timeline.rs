use crate::errors::ChronicleError;
use crate::parallel_timeline_reader::{ParallelTimelineReader, ParallelTimelineReaderOptions};
use crate::{Appendable, Offset};

pub struct ParallelTimelineOptions {
    pub key_compact: bool,
}

pub struct ParallelTimeline {}

impl ParallelTimeline {
    pub fn new(options: ParallelTimelineOptions) -> Self {
        todo!()
    }

    pub async fn open_reader(
        &self,
        reader_name: String,
        options: ParallelTimelineReaderOptions,
    ) -> Result<ParallelTimelineReader, ChronicleError> {
        todo!()
    }

    pub async fn drop(&self) -> Result<(), ChronicleError> {
        todo!()
    }
}

impl Appendable for ParallelTimeline {
    async fn append(&self, payload: Vec<u8>) -> Result<Offset, ChronicleError> {
        todo!()
    }
}
