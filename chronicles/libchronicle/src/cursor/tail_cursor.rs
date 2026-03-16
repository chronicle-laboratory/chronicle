use crate::error::ChronicleError;
use crate::{Cursor, Offset};
use std::time::Duration;

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(500);
const MAX_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// A cursor that polls for new events instead of returning `None` when caught up.
///
/// Wraps any `Cursor` implementation and re-polls with exponential backoff
/// when the underlying cursor returns `None` (end of stream). This provides
/// a "tail -f" style experience for consuming timelines.
pub struct TailCursor<C: Cursor> {
    inner: C,
    poll_interval: Duration,
    current_backoff: Duration,
}

impl<C: Cursor> TailCursor<C> {
    pub fn new(inner: C) -> Self {
        Self {
            inner,
            poll_interval: DEFAULT_POLL_INTERVAL,
            current_backoff: DEFAULT_POLL_INTERVAL,
        }
    }

    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self.current_backoff = interval;
        self
    }
}

#[async_trait::async_trait]
impl<C: Cursor + Send> Cursor for TailCursor<C> {
    /// Fetch the next event. Blocks (polls) when caught up instead of returning None.
    async fn fetch(&mut self) -> Result<Option<(Offset, Vec<u8>)>, ChronicleError> {
        loop {
            match self.inner.fetch().await? {
                Some(item) => {
                    // Reset backoff on successful read.
                    self.current_backoff = self.poll_interval;
                    return Ok(Some(item));
                }
                None => {
                    // No more events — wait and retry.
                    tokio::time::sleep(self.current_backoff).await;
                    self.current_backoff =
                        (self.current_backoff * 2).min(MAX_POLL_INTERVAL);
                    // Reset the inner stream so it re-opens on next fetch.
                    let pos = self.inner.position();
                    self.inner.seek(pos);
                }
            }
        }
    }

    fn seek(&mut self, offset: i64) {
        self.inner.seek(offset);
        self.current_backoff = self.poll_interval;
    }

    fn position(&self) -> i64 {
        self.inner.position()
    }
}
