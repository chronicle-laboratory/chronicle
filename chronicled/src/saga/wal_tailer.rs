use crate::error::unit_error::UnitError;
use crate::wal::wal::{Wal, WalOptions};
use chronicle_proto::pb_ext::Event;
use futures_util::StreamExt;
use prost::Message;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Cursor tracking WAL consumption progress.
pub struct WalCursor {
    /// Last fully processed WAL segment ID.
    pub segment_id: AtomicU64,
}

impl WalCursor {
    pub fn new(segment_id: u64) -> Self {
        Self {
            segment_id: AtomicU64::new(segment_id),
        }
    }

    pub fn advance(&self, segment_id: u64) {
        self.segment_id.fetch_max(segment_id, Ordering::Relaxed);
    }

    pub fn current(&self) -> u64 {
        self.segment_id.load(Ordering::Relaxed)
    }
}

/// Callback trait for processing events read from the WAL.
#[async_trait::async_trait]
pub trait WalEventSink: Send + Sync {
    /// Called for each event decoded from the WAL.
    async fn on_event(&self, event: Event) -> Result<(), UnitError>;

    /// Called when a batch of events has been fully consumed (e.g. end of segment).
    /// Implementations can flush buffers or checkpoint progress here.
    async fn on_batch_complete(&self) -> Result<(), UnitError> {
        Ok(())
    }
}

/// Tails the WAL directory, reading new records and forwarding decoded events
/// to the provided sink.
pub struct WalTailer {
    wal_dir: String,
    io_mode: crate::option::unit_options::IoMode,
    cursor: WalCursor,
    poll_interval: Duration,
}

impl WalTailer {
    pub fn new(
        wal_dir: String,
        io_mode: crate::option::unit_options::IoMode,
        start_segment: u64,
        poll_interval: Duration,
    ) -> Self {
        Self {
            wal_dir,
            io_mode,
            cursor: WalCursor::new(start_segment),
            poll_interval,
        }
    }

    /// Run the tail loop, reading WAL records and forwarding to the sink.
    /// Blocks until the context is cancelled.
    pub async fn run(
        &self,
        context: CancellationToken,
        sink: &dyn WalEventSink,
    ) -> Result<(), UnitError> {
        info!(
            wal_dir = %self.wal_dir,
            start_segment = self.cursor.current(),
            "saga wal tailer starting"
        );

        loop {
            let wal = Wal::new(WalOptions {
                dir: self.wal_dir.clone(),
                max_segment_size: None,
                io_mode: self.io_mode,
            })
            .await?;

            let from = self.cursor.current();
            let mut stream = wal.read_stream_from(from).await;
            let mut count = 0u64;

            loop {
                tokio::select! {
                    _ = context.cancelled() => {
                        info!(events = count, "saga wal tailer stopped");
                        return Ok(());
                    }
                    record = stream.next() => {
                        match record {
                            Some(Ok(data)) => {
                                if let Ok(event) = Event::decode(data.as_slice()) {
                                    if let Err(e) = sink.on_event(event).await {
                                        warn!(error = ?e, "saga sink error processing event");
                                    }
                                    count += 1;
                                }
                            }
                            Some(Err(e)) => {
                                warn!(error = ?e, "saga wal tailer read error");
                                break;
                            }
                            None => {
                                // Reached end of current WAL segments.
                                break;
                            }
                        }
                    }
                }
            }

            drop(stream);
            sink.on_batch_complete().await?;

            // Wait before polling for new segments.
            tokio::select! {
                _ = context.cancelled() => {
                    info!(events = count, "saga wal tailer stopped");
                    return Ok(());
                }
                _ = tokio::time::sleep(self.poll_interval) => {}
            }
        }
    }
}
