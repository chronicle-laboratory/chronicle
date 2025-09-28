use std::collections::HashSet;
use std::sync::Arc;

use chronicle_proto::pb_ext::Event;
use futures_util::Stream;
use tracing::warn;

use crate::storage::TimelineReader;
use crate::storage::segment::manager::SegmentManager;
use crate::storage::index::Storage;
use crate::storage::write_cache::WriteCache;

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

fn yield_events(events: Vec<Event>, start_offset: i64) -> impl Stream<Item = (i64, i64, Vec<Event>)> {
    async_stream::stream! {
        let mut batch = Vec::new();
        let mut last_offset = start_offset;
        for event in events {
            last_offset = event.offset;
            batch.push(event);
            if batch.len() >= 100 {
                yield (last_offset, last_offset, std::mem::take(&mut batch));
            }
        }
        if !batch.is_empty() {
            yield (last_offset, last_offset, batch);
        }
    }
}

impl TimelineReader for LevelIterator {
    fn fetch_batches(
        &self,
        timeline_id: i64,
        start_offset: i64,
        end_offset: i64,
    ) -> impl Stream<Item = (i64, i64, Vec<Event>)> {
        let wc_events = self.write_cache.scan(timeline_id, start_offset, end_offset);
        let index_entries = self.index.scan_index(timeline_id, start_offset, end_offset);

        let segment_manager = self.segment_manager.clone();

        async_stream::stream! {
            if index_entries.is_empty() {
                let mut s = std::pin::pin!(yield_events(wc_events, start_offset));
                while let Some(item) = futures_util::StreamExt::next(&mut s).await {
                    yield item;
                }
                return;
            }

            if wc_events.is_empty() {
                let mut batch = Vec::new();
                let mut last_offset = start_offset;
                for (_, entry) in index_entries {
                    match segment_manager.read_event(&entry) {
                        Ok(event) => {
                            last_offset = event.offset;
                            batch.push(event);
                            if batch.len() >= 100 {
                                yield (last_offset, last_offset, std::mem::take(&mut batch));
                            }
                        }
                        Err(e) => {
                            warn!(error = ?e, "failed to read event from segment");
                        }
                    }
                }
                if !batch.is_empty() {
                    yield (last_offset, last_offset, batch);
                }
                return;
            }

            let wc_offsets: HashSet<i64> = wc_events.iter().map(|e| e.offset).collect();

            let mut wc_iter = wc_events.into_iter().peekable();
            let mut idx_iter = index_entries.into_iter().peekable();
            let mut batch = Vec::new();
            let mut last_offset = start_offset;

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
                    Some(event) => {
                        last_offset = event.offset;
                        batch.push(event);
                        if batch.len() >= 100 {
                            yield (last_offset, last_offset, std::mem::take(&mut batch));
                        }
                    }
                    None => break,
                }
            }

            if !batch.is_empty() {
                yield (last_offset, last_offset, batch);
            }
        }
    }
}
