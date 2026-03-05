use std::collections::HashSet;
use std::sync::Arc;

use chronicle_proto::pb_ext::Event;
use tracing::warn;

use crate::storage::TimelineReader;
use crate::storage::blob::manager::SegmentManager;
use crate::storage::index::Storage;
use crate::storage::write_cache::WriteCache;

const BATCH_SIZE: usize = 100;

#[derive(Clone)]
pub struct LevelIterator {
    write_cache: WriteCache,
    index: Storage,
    segment_manager: Arc<SegmentManager>,
}

impl LevelIterator {
    pub fn new(
        write_cache: WriteCache,
        index: Storage,
        segment_manager: Arc<SegmentManager>,
    ) -> Self {
        Self {
            write_cache,
            index,
            segment_manager,
        }
    }
}

impl TimelineReader for LevelIterator {
    fn fetch_batches(
        &self,
        timeline_id: i64,
        start_offset: i64,
        end_offset: i64,
    ) -> impl Iterator<Item = (i64, i64, Vec<Event>)> {
        let wc_events = self.write_cache.scan(timeline_id, start_offset, end_offset);
        let index_entries = self.index.scan_index(timeline_id, start_offset, end_offset);
        let segment_manager = self.segment_manager.clone();

        // Merge write cache events and index entries into a single sorted event stream.
        let wc_offsets: HashSet<i64> = wc_events.iter().map(|e| e.offset).collect();

        let mut wc_iter = wc_events.into_iter().peekable();
        let mut idx_iter = index_entries.into_iter().peekable();
        let mut merged = Vec::new();

        loop {
            let next_event = match (wc_iter.peek(), idx_iter.peek()) {
                (Some(wc_event), Some(&(idx_offset, _))) => {
                    if wc_event.offset <= idx_offset {
                        if wc_event.offset == idx_offset {
                            idx_iter.next();
                        }
                        wc_iter.next()
                    } else if wc_offsets.contains(&idx_offset) {
                        idx_iter.next();
                        continue;
                    } else {
                        let (_, entry) = idx_iter.next().unwrap();
                        match segment_manager.read_event(&entry) {
                            Ok(event) => Some(event),
                            Err(e) => {
                                warn!(error = ?e, "failed to read event from segment");
                                continue;
                            }
                        }
                    }
                }
                (Some(_), None) => wc_iter.next(),
                (None, Some(_)) => {
                    let (idx_offset, entry) = idx_iter.next().unwrap();
                    if wc_offsets.contains(&idx_offset) {
                        continue;
                    }
                    match segment_manager.read_event(&entry) {
                        Ok(event) => Some(event),
                        Err(e) => {
                            warn!(error = ?e, "failed to read event from segment");
                            continue;
                        }
                    }
                }
                (None, None) => None,
            };

            match next_event {
                Some(event) => merged.push(event),
                None => break,
            }
        }

        BatchIterator {
            events: merged,
            pos: 0,
            start_offset,
        }
    }
}

struct BatchIterator {
    events: Vec<Event>,
    pos: usize,
    start_offset: i64,
}

impl Iterator for BatchIterator {
    type Item = (i64, i64, Vec<Event>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.events.len() {
            return None;
        }

        let end = (self.pos + BATCH_SIZE).min(self.events.len());
        let batch: Vec<Event> = self.events[self.pos..end].to_vec();
        self.pos = end;

        let last_offset = batch.last().map(|e| e.offset).unwrap_or(self.start_offset);
        Some((last_offset, last_offset, batch))
    }
}
