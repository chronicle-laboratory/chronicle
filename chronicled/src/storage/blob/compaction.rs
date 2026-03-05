use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chronicle_proto::pb_ext::Event;
use prost::Message;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::error::unit_error::UnitError;
use super::manager::SegmentManager;
use super::remote::RemoteStore;
use crate::storage::index::{IndexEntry, Storage};
use crate::storage::write_cache::WriteCache;

/// Leveled compaction pipeline with per-level tasks:
///   L0 = write cache (in-memory, not managed here)
///   L1 = sealed write cache flush → blob segment
///   L2 = merge N L1 segments → fewer, larger, timeline-sorted segments
///   L3 = split L2 by timeline → one segment per timeline
///   L4 = remote (S3 offload of L3 segments)
///
/// Each level runs on its own tokio task and notifies the next level on completion.
pub struct CompactionPipeline {
    handles: Vec<JoinHandle<()>>,
}

impl CompactionPipeline {
    pub fn spawn(
        write_cache: WriteCache,
        segment_manager: Arc<SegmentManager>,
        index: Storage,
        context: CancellationToken,
        interval: Duration,
        l1_compaction_trigger: usize,
        l2_compaction_trigger: usize,
        remote_store: Option<Arc<dyn RemoteStore>>,
    ) -> Self {
        let flush_notify = write_cache.flush_notify();

        // Notify signals: each level notifies the next when it produces output
        let l2_notify = Arc::new(Notify::new());
        let l3_notify = Arc::new(Notify::new());
        let l4_notify = Arc::new(Notify::new());

        let l1_handle = {
            let segment_manager = segment_manager.clone();
            let index = index.clone();
            let context = context.clone();
            let l2_notify = l2_notify.clone();
            tokio::spawn(async move {
                let task = L1FlushTask {
                    write_cache,
                    segment_manager,
                    index,
                    flush_notify,
                    l2_notify,
                };
                task.run(context, interval).await;
            })
        };

        let l2_handle = {
            let segment_manager = segment_manager.clone();
            let index = index.clone();
            let context = context.clone();
            let l3_notify = l3_notify.clone();
            tokio::spawn(async move {
                let task = L2MergeTask {
                    segment_manager,
                    index,
                    l2_notify,
                    l3_notify,
                    trigger: l1_compaction_trigger,
                };
                task.run(context).await;
            })
        };

        let l3_handle = {
            let segment_manager = segment_manager.clone();
            let index = index.clone();
            let context = context.clone();
            let l4_notify = l4_notify.clone();
            tokio::spawn(async move {
                let task = L3SplitTask {
                    segment_manager,
                    index,
                    l3_notify,
                    l4_notify,
                    trigger: l2_compaction_trigger,
                };
                task.run(context).await;
            })
        };

        let l4_handle = if let Some(remote_store) = remote_store {
            let segment_manager = segment_manager.clone();
            let context = context.clone();
            Some(tokio::spawn(async move {
                let task = L4OffloadTask {
                    segment_manager,
                    remote_store,
                    l4_notify,
                };
                task.run(context).await;
            }))
        } else {
            None
        };

        let mut handles = vec![l1_handle, l2_handle, l3_handle];
        if let Some(h) = l4_handle {
            handles.push(h);
        }

        Self { handles }
    }

    pub async fn shutdown(self) {
        for handle in self.handles {
            if let Err(err) = handle.await {
                warn!(error = ?err, "compaction task join error");
            }
        }
    }
}

// ─── L1: Flush sealed write cache → blob segment ───

struct L1FlushTask {
    write_cache: WriteCache,
    segment_manager: Arc<SegmentManager>,
    index: Storage,
    flush_notify: Arc<Notify>,
    l2_notify: Arc<Notify>,
}

impl L1FlushTask {
    async fn run(&self, context: CancellationToken, interval: Duration) {
        let mut ticker = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = context.cancelled() => {
                    if let Err(e) = self.flush().await {
                        warn!(error = ?e, "final L1 flush failed");
                    }
                    info!("L1 flush task stopped");
                    break;
                }
                _ = ticker.tick() => {
                    if let Err(e) = self.flush().await {
                        warn!(error = ?e, "L1 flush failed");
                    }
                }
                _ = self.flush_notify.notified() => {
                    if let Err(e) = self.flush().await {
                        warn!(error = ?e, "L1 flush failed");
                    }
                }
            }
        }
    }

    async fn flush(&self) -> Result<(), UnitError> {
        let sealed = match self.write_cache.sealed_data() {
            Some(s) => s,
            None => return Ok(()),
        };

        if sealed.index.is_empty() {
            self.write_cache.clear_sealed();
            return Ok(());
        }

        let entries_count = sealed.index.len();

        let mut writer = self.segment_manager.new_writer_at_level(1).await?;
        let segment_id = writer.segment_id();
        let mut index_entries = Vec::with_capacity(entries_count);

        for entry in sealed.index.iter() {
            let &(timeline_id, offset) = entry.key();
            let &idx = entry.value();
            let data = &sealed.buffer[idx as usize];
            if let Ok(event) = Event::decode(data.as_slice()) {
                let (byte_offset, length) = writer.write_entry(&event).await?;
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

        let size = writer.size();
        let entry_count = writer.entry_count();
        writer.finish().await?;

        self.segment_manager.update_meta(segment_id, size, entry_count);
        self.index.put_index_batch(&index_entries)?;
        self.write_cache.clear_sealed();

        info!(segment_id = segment_id, entries = entries_count, "L1 flush complete");

        // Notify L2 that new L1 segments are available
        self.l2_notify.notify_one();

        Ok(())
    }
}

// ─── L2: Merge N L1 segments → timeline-sorted segment ───

struct L2MergeTask {
    segment_manager: Arc<SegmentManager>,
    index: Storage,
    l2_notify: Arc<Notify>,
    l3_notify: Arc<Notify>,
    trigger: usize,
}

impl L2MergeTask {
    async fn run(&self, context: CancellationToken) {
        loop {
            tokio::select! {
                _ = context.cancelled() => {
                    // Final merge attempt
                    let _ = self.merge().await;
                    info!("L2 merge task stopped");
                    break;
                }
                _ = self.l2_notify.notified() => {
                    if let Err(e) = self.merge().await {
                        warn!(error = ?e, "L2 merge failed");
                    }
                }
            }
        }
    }

    async fn merge(&self) -> Result<(), UnitError> {
        let l1_segments = self.segment_manager.segments_at_level(1);
        if l1_segments.len() < self.trigger {
            return Ok(());
        }

        let source_ids: HashSet<u64> = l1_segments.iter().map(|m| m.id).collect();
        let entries = self.index.scan_by_segment_ids(&source_ids);

        if entries.is_empty() {
            let ids: Vec<u64> = source_ids.into_iter().collect();
            self.segment_manager.remove_segments(&ids);
            return Ok(());
        }

        let mut sorted = entries;
        sorted.sort_by_key(|&((tid, off), _)| (tid, off));

        let mut writer = self.segment_manager.new_writer_at_level(2).await?;
        let new_segment_id = writer.segment_id();
        let mut new_index_entries = Vec::with_capacity(sorted.len());

        for ((timeline_id, offset), old_entry) in &sorted {
            let event = self.segment_manager.read_event(old_entry)?;
            let (byte_offset, length) = writer.write_entry(&event).await?;
            new_index_entries.push((
                (*timeline_id, *offset),
                IndexEntry {
                    segment_id: new_segment_id,
                    byte_offset,
                    length,
                },
            ));
        }

        let size = writer.size();
        let entry_count = writer.entry_count();
        writer.finish().await?;
        self.segment_manager.update_meta(new_segment_id, size, entry_count);

        self.index.put_index_batch(&new_index_entries)?;
        let ids: Vec<u64> = source_ids.into_iter().collect();
        self.segment_manager.remove_segments(&ids);

        info!(
            source_count = l1_segments.len(),
            entries = new_index_entries.len(),
            new_segment_id = new_segment_id,
            "L1 → L2 merge complete"
        );

        // Notify L3 that new L2 segments are available
        self.l3_notify.notify_one();

        Ok(())
    }
}

// ─── L3: Split L2 segments by timeline ───

struct L3SplitTask {
    segment_manager: Arc<SegmentManager>,
    index: Storage,
    l3_notify: Arc<Notify>,
    l4_notify: Arc<Notify>,
    trigger: usize,
}

impl L3SplitTask {
    async fn run(&self, context: CancellationToken) {
        loop {
            tokio::select! {
                _ = context.cancelled() => {
                    let _ = self.split().await;
                    info!("L3 split task stopped");
                    break;
                }
                _ = self.l3_notify.notified() => {
                    if let Err(e) = self.split().await {
                        warn!(error = ?e, "L3 split failed");
                    }
                }
            }
        }
    }

    async fn split(&self) -> Result<(), UnitError> {
        let l2_segments = self.segment_manager.segments_at_level(2);
        if l2_segments.len() < self.trigger {
            return Ok(());
        }

        let source_ids: HashSet<u64> = l2_segments.iter().map(|m| m.id).collect();
        let entries = self.index.scan_by_segment_ids(&source_ids);

        if entries.is_empty() {
            let ids: Vec<u64> = source_ids.into_iter().collect();
            self.segment_manager.remove_segments(&ids);
            return Ok(());
        }

        let mut by_timeline: HashMap<i64, Vec<((i64, i64), IndexEntry)>> = HashMap::new();
        for entry in entries {
            by_timeline.entry(entry.0.0).or_default().push(entry);
        }

        let mut all_new_entries = Vec::new();

        for (timeline_id, mut timeline_entries) in by_timeline {
            timeline_entries.sort_by_key(|&((_, off), _)| off);

            let mut writer = self.segment_manager.new_writer_at_level(3).await?;
            let new_segment_id = writer.segment_id();

            for ((tid, offset), old_entry) in &timeline_entries {
                let event = self.segment_manager.read_event(old_entry)?;
                let (byte_offset, length) = writer.write_entry(&event).await?;
                all_new_entries.push((
                    (*tid, *offset),
                    IndexEntry {
                        segment_id: new_segment_id,
                        byte_offset,
                        length,
                    },
                ));
            }

            let size = writer.size();
            let entry_count = writer.entry_count();
            writer.finish().await?;
            self.segment_manager.update_meta(new_segment_id, size, entry_count);

            info!(
                timeline_id = timeline_id,
                entries = timeline_entries.len(),
                new_segment_id = new_segment_id,
                "L3 segment created for timeline"
            );
        }

        self.index.put_index_batch(&all_new_entries)?;
        let ids: Vec<u64> = source_ids.into_iter().collect();
        self.segment_manager.remove_segments(&ids);

        info!(source_count = l2_segments.len(), "L2 → L3 split complete");

        // Notify L4 that new L3 segments are available
        self.l4_notify.notify_one();

        Ok(())
    }
}

// ─── L4: Offload L3 segments to remote storage ───

struct L4OffloadTask {
    segment_manager: Arc<SegmentManager>,
    remote_store: Arc<dyn RemoteStore>,
    l4_notify: Arc<Notify>,
}

impl L4OffloadTask {
    async fn run(&self, context: CancellationToken) {
        loop {
            tokio::select! {
                _ = context.cancelled() => {
                    let _ = self.offload().await;
                    info!("L4 offload task stopped");
                    break;
                }
                _ = self.l4_notify.notified() => {
                    if let Err(e) = self.offload().await {
                        warn!(error = ?e, "L4 offload failed");
                    }
                }
            }
        }
    }

    async fn offload(&self) -> Result<(), UnitError> {
        let l3_segments = self.segment_manager.segments_at_level(3);
        if l3_segments.is_empty() {
            return Ok(());
        }

        for meta in &l3_segments {
            let local_path = match self.segment_manager.segment_path_for(meta.id) {
                Some(p) => p,
                None => continue,
            };

            if !local_path.exists() {
                continue;
            }

            let filename = local_path.file_name()
                .and_then(|f| f.to_str())
                .ok_or_else(|| UnitError::Storage("invalid segment path".into()))?;

            let key = format!("chronicle/segments/{}", filename);

            self.remote_store.upload(&local_path, &key).await?;
            self.segment_manager.mark_remote(meta.id, key.clone());

            info!(
                segment_id = meta.id,
                key = %key,
                "L3 → L4 segment offloaded to remote"
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::option::unit_options::IoMode;
    use crate::storage::index::StorageOptions;

    // Helper to create individual level tasks for direct testing
    struct TestHarness {
        write_cache: WriteCache,
        segment_manager: Arc<SegmentManager>,
        index: Storage,
        l1: L1FlushTask,
        l2: L2MergeTask,
        l3: L3SplitTask,
    }

    fn setup_test(
        dir: &std::path::Path,
        l1_trigger: usize,
        l2_trigger: usize,
    ) -> TestHarness {
        let segments_dir = dir.join("segments");
        let index_dir = dir.join("index");

        let write_cache = WriteCache::new(64 * 1024 * 1024);
        let segment_manager = Arc::new(
            SegmentManager::recover(segments_dir, IoMode::Basic).unwrap(),
        );
        let index = Storage::new(StorageOptions {
            path: index_dir.to_string_lossy().to_string(),
        })
        .unwrap();

        let flush_notify = write_cache.flush_notify();
        let l2_notify = Arc::new(Notify::new());
        let l3_notify = Arc::new(Notify::new());
        let l4_notify = Arc::new(Notify::new());

        let l1 = L1FlushTask {
            write_cache: write_cache.clone(),
            segment_manager: segment_manager.clone(),
            index: index.clone(),
            flush_notify,
            l2_notify: l2_notify.clone(),
        };

        let l2 = L2MergeTask {
            segment_manager: segment_manager.clone(),
            index: index.clone(),
            l2_notify,
            l3_notify: l3_notify.clone(),
            trigger: l1_trigger,
        };

        let l3 = L3SplitTask {
            segment_manager: segment_manager.clone(),
            index: index.clone(),
            l3_notify,
            l4_notify,
            trigger: l2_trigger,
        };

        TestHarness {
            write_cache,
            segment_manager,
            index,
            l1,
            l2,
            l3,
        }
    }

    #[tokio::test]
    async fn test_flush_to_l1() {
        let dir = tempfile::tempdir().unwrap();
        let h = setup_test(dir.path(), 4, 4);

        for i in 0..5 {
            let event = Event {
                timeline_id: 1,
                term: 1,
                offset: i,
                payload: Some(format!("data_{}", i).into_bytes().into()),
                crc32: None,
                timestamp: i * 100,
            };
            h.write_cache.put_direct(event, true);
        }

        h.write_cache.try_seal();
        h.l1.flush().await.unwrap();

        assert_eq!(h.segment_manager.segments_at_level(1).len(), 1);
        let entries = h.index.scan_index(1, 0, 10);
        assert_eq!(entries.len(), 5);
    }

    #[tokio::test]
    async fn test_l1_to_l2_merge() {
        let dir = tempfile::tempdir().unwrap();
        let h = setup_test(dir.path(), 4, 100);

        for batch in 0..4 {
            for i in 0..3 {
                let offset = batch * 3 + i;
                let event = Event {
                    timeline_id: 1,
                    term: 1,
                    offset,
                    payload: Some(format!("batch{}_{}", batch, i).into_bytes().into()),
                    crc32: None,
                    timestamp: offset * 100,
                };
                h.write_cache.put_direct(event, true);
            }
            h.write_cache.try_seal();
            h.l1.flush().await.unwrap();
        }

        assert_eq!(h.segment_manager.segments_at_level(1).len(), 4);

        h.l2.merge().await.unwrap();

        assert_eq!(h.segment_manager.segments_at_level(1).len(), 0);
        assert_eq!(h.segment_manager.segments_at_level(2).len(), 1);

        let entries = h.index.scan_index(1, 0, 100);
        assert_eq!(entries.len(), 12);

        for (offset, entry) in &entries {
            let event = h.segment_manager.read_event(entry).unwrap();
            assert_eq!(event.offset, *offset);
            assert_eq!(event.timeline_id, 1);
        }
    }

    #[tokio::test]
    async fn test_l2_to_l3_split() {
        let dir = tempfile::tempdir().unwrap();
        let h = setup_test(dir.path(), 2, 2);

        for batch in 0..2 {
            for timeline in 1..=3i64 {
                for i in 0..2 {
                    let offset = batch * 2 + i;
                    let event = Event {
                        timeline_id: timeline,
                        term: 1,
                        offset,
                        payload: Some(format!("t{}_b{}_{}", timeline, batch, i).into_bytes().into()),
                        crc32: None,
                        timestamp: offset * 100,
                    };
                    h.write_cache.put_direct(event, true);
                }
            }
            h.write_cache.try_seal();
            h.l1.flush().await.unwrap();
        }

        h.l2.merge().await.unwrap();
        assert!(h.segment_manager.segments_at_level(1).is_empty());

        for batch in 0..2 {
            for timeline in 1..=3i64 {
                let offset = 10 + batch;
                let event = Event {
                    timeline_id: timeline,
                    term: 1,
                    offset,
                    payload: Some(format!("extra_t{}_{}", timeline, batch).into_bytes().into()),
                    crc32: None,
                    timestamp: offset * 100,
                };
                h.write_cache.put_direct(event, true);
            }
            h.write_cache.try_seal();
            h.l1.flush().await.unwrap();
        }
        h.l2.merge().await.unwrap();

        let l2_count = h.segment_manager.segments_at_level(2).len();
        assert!(l2_count >= 2, "expected >= 2 L2 segments, got {}", l2_count);

        h.l3.split().await.unwrap();

        assert_eq!(h.segment_manager.segments_at_level(2).len(), 0);
        assert_eq!(h.segment_manager.segments_at_level(3).len(), 3);

        for timeline in 1..=3i64 {
            let entries = h.index.scan_index(timeline, 0, 100);
            assert!(!entries.is_empty(), "timeline {} should have entries", timeline);
            for (_, entry) in &entries {
                let event = h.segment_manager.read_event(entry).unwrap();
                assert_eq!(event.timeline_id, timeline);
            }
        }
    }

    #[tokio::test]
    async fn test_recovery_with_leveled_filenames() {
        let dir = tempfile::tempdir().unwrap();
        let segments_dir = dir.path().join("segments");

        {
            let mgr = SegmentManager::recover(segments_dir.clone(), IoMode::Basic).unwrap();
            let w = mgr.new_writer_at_level(1).await.unwrap();
            w.finish().await.unwrap();
            let w = mgr.new_writer_at_level(2).await.unwrap();
            w.finish().await.unwrap();
            let w = mgr.new_writer_at_level(3).await.unwrap();
            w.finish().await.unwrap();
        }

        let mgr = SegmentManager::recover(segments_dir, IoMode::Basic).unwrap();
        assert_eq!(mgr.segments_at_level(1).len(), 1);
        assert_eq!(mgr.segments_at_level(2).len(), 1);
        assert_eq!(mgr.segments_at_level(3).len(), 1);
    }

    #[tokio::test]
    async fn test_full_compaction_pipeline() {
        let dir = tempfile::tempdir().unwrap();
        let h = setup_test(dir.path(), 2, 2);

        for batch in 0..4 {
            for timeline in 1..=2i64 {
                let event = Event {
                    timeline_id: timeline,
                    term: 1,
                    offset: batch,
                    payload: Some(format!("t{}_off{}", timeline, batch).into_bytes().into()),
                    crc32: None,
                    timestamp: batch * 100,
                };
                h.write_cache.put_direct(event, true);
            }
            h.write_cache.try_seal();
            h.l1.flush().await.unwrap();
            h.l2.merge().await.unwrap();
            h.l3.split().await.unwrap();
        }

        let l3 = h.segment_manager.segments_at_level(3);
        assert_eq!(l3.len(), 2, "expected 2 L3 segments (one per timeline)");

        for timeline in 1..=2i64 {
            let entries = h.index.scan_index(timeline, 0, 10);
            assert_eq!(entries.len(), 4);
            for (_, entry) in &entries {
                let event = h.segment_manager.read_event(entry).unwrap();
                assert_eq!(event.timeline_id, timeline);
            }
        }
    }

    #[tokio::test]
    async fn test_pipeline_spawn_and_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let segments_dir = dir.path().join("segments");
        let index_dir = dir.path().join("index");

        let write_cache = WriteCache::new(64 * 1024 * 1024);
        let segment_manager = Arc::new(
            SegmentManager::recover(segments_dir, IoMode::Basic).unwrap(),
        );
        let index = Storage::new(StorageOptions {
            path: index_dir.to_string_lossy().to_string(),
        })
        .unwrap();

        let context = CancellationToken::new();

        let pipeline = CompactionPipeline::spawn(
            write_cache.clone(),
            segment_manager.clone(),
            index.clone(),
            context.clone(),
            Duration::from_millis(50),
            2,
            2,
            None,
        );

        // Write some data and seal to trigger the pipeline
        for i in 0..3 {
            let event = Event {
                timeline_id: 1,
                term: 1,
                offset: i,
                payload: Some(format!("data_{}", i).into_bytes().into()),
                crc32: None,
                timestamp: i * 100,
            };
            write_cache.put_direct(event, true);
        }
        write_cache.try_seal();

        // Give pipeline time to process
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Should have flushed to L1
        assert!(h_segment_manager_has_l1(&segment_manager));

        context.cancel();
        pipeline.shutdown().await;
    }

    fn h_segment_manager_has_l1(mgr: &SegmentManager) -> bool {
        !mgr.segments_at_level(1).is_empty()
    }
}
