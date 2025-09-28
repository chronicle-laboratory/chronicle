use std::sync::Arc;
use std::time::Duration;

use chronicle_proto::pb_ext::Event;
use prost::Message;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::error::unit_error::UnitError;
use crate::storage::segment::manager::SegmentManager;
use crate::storage::index::{IndexEntry, Storage};
use crate::storage::write_cache::WriteCache;

pub struct CompactionTask {
    write_cache: WriteCache,
    segment_manager: Arc<SegmentManager>,
    index: Storage,
    context: CancellationToken,
    interval: Duration,
    flush_notify: Arc<Notify>,
}

impl CompactionTask {
    pub fn new(
        write_cache: WriteCache,
        segment_manager: Arc<SegmentManager>,
        index: Storage,
        context: CancellationToken,
        interval: Duration,
    ) -> Self {
        let flush_notify = write_cache.flush_notify();
        Self {
            write_cache,
            segment_manager,
            index,
            context,
            interval,
            flush_notify,
        }
    }

    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.interval);
            loop {
                tokio::select! {
                    _ = self.context.cancelled() => {
                        if let Err(e) = self.compact() {
                            warn!(error = ?e, "final compaction failed");
                        }
                        info!("compaction task stopped");
                        break;
                    }
                    _ = interval.tick() => {
                        if let Err(e) = self.compact() {
                            warn!(error = ?e, "compaction failed");
                        }
                    }
                    _ = self.flush_notify.notified() => {
                        if let Err(e) = self.compact() {
                            warn!(error = ?e, "flush-triggered compaction failed");
                        }
                    }
                }
            }
        })
    }

    fn compact(&self) -> Result<(), UnitError> {
        let sealed = match self.write_cache.sealed_data() {
            Some(s) => s,
            None => return Ok(()),
        };

        if sealed.index.is_empty() {
            self.write_cache.clear_sealed();
            return Ok(());
        }

        let entries_count = sealed.index.len();

        let mut writer = self.segment_manager.new_writer()?;
        let segment_id = writer.segment_id();
        let mut index_entries = Vec::with_capacity(entries_count);

        for entry in sealed.index.iter() {
            let &(timeline_id, offset) = entry.key();
            let &idx = entry.value();
            let data = &sealed.buffer[idx as usize];
            if let Ok(event) = Event::decode(data.as_slice()) {
                let (byte_offset, length) = writer.write_entry(&event)?;
                index_entries.push((
                    (timeline_id, offset),
                    IndexEntry {
                        segment_id,
                        byte_offset,
                        length,
                    },
                ));
            }
        }

        writer.finish()?;
        self.index.put_index_batch(&index_entries)?;
        self.write_cache.clear_sealed();

        info!(
            segment_id = segment_id,
            entries = entries_count,
            "compaction complete"
        );

        Ok(())
    }
}
