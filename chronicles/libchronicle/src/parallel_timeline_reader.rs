use crate::error::ChronicleError;
use crate::unit_client::UnitClient;
use crate::{Fetchable, Offset, Seekable};
use async_stream::stream;
use futures_util::Stream;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

pub struct ParallelTimelineReaderOptions {
    pub cursor_name: Option<String>,
    pub start_offset: Option<i64>,
}

struct ReaderInner {
    timeline_id: i64,
    ensemble: Vec<String>,
    cursor_name: Option<String>,
    position: AtomicI64,
}

pub struct ParallelTimelineReader {
    inner: Arc<ReaderInner>,
    unit_clients: Vec<UnitClient>,
}

impl ParallelTimelineReader {
    pub fn new(
        timeline_id: i64,
        ensemble: Vec<String>,
        unit_clients: Vec<UnitClient>,
        options: ParallelTimelineReaderOptions,
    ) -> Self {
        let position = AtomicI64::new(options.start_offset.unwrap_or(0));
        Self {
            inner: Arc::new(ReaderInner {
                timeline_id,
                ensemble,
                cursor_name: options.cursor_name,
                position,
            }),
            unit_clients,
        }
    }
}

impl Fetchable for ParallelTimelineReader {
    async fn fetch(
        &self,
    ) -> Result<impl Stream<Item = Result<(Offset, Vec<u8>), ChronicleError>>, ChronicleError>
    {
        Ok(stream! {
            yield Err(ChronicleError::Unsupported("not yet implemented".into()));
        })
    }

    async fn fetch_next(&self) -> Result<(Offset, Vec<u8>), ChronicleError> {
        todo!()
    }
}

impl Seekable for ParallelTimelineReader {
    async fn seek(&self, offset: i64) -> Result<(), ChronicleError> {
        self.inner.position.store(offset, Ordering::Release);
        Ok(())
    }
}
