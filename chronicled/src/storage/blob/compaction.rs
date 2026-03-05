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

/// Leveled compaction pipeline:
///   L0 = write cache (in-memory, not managed here)
///   L1 = sealed write cache flush → blob segment
///   L2 = merge N L1 segments → fewer, larger, timeline-sorted segments
///   L3 = split L2 by timeline → one segment per timeline
///   L4 = remote (S3 offload of L3 segments)
pub struct CompactionTask {
    write_cache: WriteCache,
    segment_manager: Arc<SegmentManager>,
    index: Storage,
    context: CancellationToken,
    interval: Duration,
    flush_notify: Arc<Notify>,
    l1_compaction_trigger: usize,
    l2_compaction_trigger: usize,
    remote_store: Option<Arc<dyn RemoteStore>>,
}

impl CompactionTask {
    pub fn new(
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
        Self {
            write_cache,
            segment_manager,
            index,
            context,
            interval,
            flush_notify,
            l1_compaction_trigger,
            l2_compaction_trigger,
            remote_store,
        }
    }

    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.interval);
            loop {
                tokio::select! {
                    _ = self.context.cancelled() => {
                        if let Err(e) = self.compact().await {
                            warn!(error = ?e, "final compaction failed");
                        }
                        info!("compaction task stopped");
                        break;
                    }
                    _ = interval.tick() => {
                        if let Err(e) = self.compact().await {
                            warn!(error = ?e, "compaction failed");
                        }
                    }
                    _ = self.flush_notify.notified() => {
                        if let Err(e) = self.compact().await {
                            warn!(error = ?e, "flush-triggered compaction failed");
                        }
                    }
                }
            }
        })
    }

    async fn compact(&self) -> Result<(), UnitError> {
        // L0 → L1: flush sealed write cache to blob segment
        self.flush_to_l1().await?;

        // L1 → L2: merge N L1 segments into timeline-sorted segment
        self.merge_l1_to_l2().await?;

        // L2 → L3: split by timeline
        self.split_l2_to_l3().await?;

        // L3 → L4: offload to remote
        self.offload_l3_to_remote().await?;

        Ok(())
    }

    /// L0 → L1: Flush sealed write cache to a new L1 blob segment.
    async fn flush_to_l1(&self) -> Result<(), UnitError> {
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

        info!(
            segment_id = segment_id,
            entries = entries_count,
            "L1 flush complete"
        );

        Ok(())
    }

    /// L1 → L2: Merge N L1 segments into fewer, larger L2 segments,
    /// sorted by (timeline_id, offset) for best-effort timeline grouping.
    async fn merge_l1_to_l2(&self) -> Result<(), UnitError> {
        let l1_segments = self.segment_manager.segments_at_level(1);
        if l1_segments.len() < self.l1_compaction_trigger {
            return Ok(());
        }

        let source_ids: HashSet<u64> = l1_segments.iter().map(|m| m.id).collect();
        let entries = self.index.scan_by_segment_ids(&source_ids);

        if entries.is_empty() {
            let ids: Vec<u64> = source_ids.into_iter().collect();
            self.segment_manager.remove_segments(&ids);
            return Ok(());
        }

        // Sort by (timeline_id, offset) for best-effort timeline grouping
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

        // Atomic index update, then remove old L1 segments
        self.index.put_index_batch(&new_index_entries)?;
        let ids: Vec<u64> = source_ids.into_iter().collect();
        self.segment_manager.remove_segments(&ids);

        info!(
            source_count = l1_segments.len(),
            entries = new_index_entries.len(),
            new_segment_id = new_segment_id,
            "L1 → L2 merge complete"
        );

        Ok(())
    }

    /// L2 → L3: Split L2 segments by timeline — one L3 segment per timeline.
    async fn split_l2_to_l3(&self) -> Result<(), UnitError> {
        let l2_segments = self.segment_manager.segments_at_level(2);
        if l2_segments.len() < self.l2_compaction_trigger {
            return Ok(());
        }

        let source_ids: HashSet<u64> = l2_segments.iter().map(|m| m.id).collect();
        let entries = self.index.scan_by_segment_ids(&source_ids);

        if entries.is_empty() {
            let ids: Vec<u64> = source_ids.into_iter().collect();
            self.segment_manager.remove_segments(&ids);
            return Ok(());
        }

        // Group entries by timeline_id
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

        // Atomic index update, then remove old L2 segments
        self.index.put_index_batch(&all_new_entries)?;
        let ids: Vec<u64> = source_ids.into_iter().collect();
        self.segment_manager.remove_segments(&ids);

        info!(
            source_count = l2_segments.len(),
            "L2 → L3 split complete"
        );

        Ok(())
    }

    /// L3 → L4: Offload per-timeline L3 segments to remote storage (S3).
    async fn offload_l3_to_remote(&self) -> Result<(), UnitError> {
        let remote_store = match &self.remote_store {
            Some(o) => o.clone(),
            None => return Ok(()),
        };

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

            // Upload to S3
            remote_store.upload(&local_path, &key).await?;

            // Mark as remote — deletes local file, keeps meta with Remote location.
            // Index entries remain valid — reads fall back to S3 via LRU cache.
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

    fn setup_test(
        dir: &std::path::Path,
        l1_trigger: usize,
        l2_trigger: usize,
    ) -> (WriteCache, Arc<SegmentManager>, Storage, CompactionTask) {
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

        let context = CancellationToken::new();
        let task = CompactionTask::new(
            write_cache.clone(),
            segment_manager.clone(),
            index.clone(),
            context,
            Duration::from_secs(3600),
            l1_trigger,
            l2_trigger,
            None,
        );

        (write_cache, segment_manager, index, task)
    }

    #[tokio::test]
    async fn test_flush_to_l1() {
        let dir = tempfile::tempdir().unwrap();
        let (write_cache, segment_manager, index, task) = setup_test(dir.path(), 4, 4);

        for i in 0..5 {
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
        task.flush_to_l1().await.unwrap();

        assert_eq!(segment_manager.segments_at_level(1).len(), 1);
        let entries = index.scan_index(1, 0, 10);
        assert_eq!(entries.len(), 5);
    }

    #[tokio::test]
    async fn test_l1_to_l2_merge() {
        let dir = tempfile::tempdir().unwrap();
        let (write_cache, segment_manager, index, task) = setup_test(dir.path(), 4, 100);

        // Create 4 L1 segments (trigger threshold)
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
                write_cache.put_direct(event, true);
            }
            write_cache.try_seal();
            task.flush_to_l1().await.unwrap();
        }

        assert_eq!(segment_manager.segments_at_level(1).len(), 4);

        task.merge_l1_to_l2().await.unwrap();

        assert_eq!(segment_manager.segments_at_level(1).len(), 0);
        assert_eq!(segment_manager.segments_at_level(2).len(), 1);

        let entries = index.scan_index(1, 0, 100);
        assert_eq!(entries.len(), 12);

        for (offset, entry) in &entries {
            let event = segment_manager.read_event(entry).unwrap();
            assert_eq!(event.offset, *offset);
            assert_eq!(event.timeline_id, 1);
        }
    }

    #[tokio::test]
    async fn test_l2_to_l3_split() {
        let dir = tempfile::tempdir().unwrap();
        let (write_cache, segment_manager, index, task) = setup_test(dir.path(), 2, 2);

        // Create events for 3 timelines across batches
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
                    write_cache.put_direct(event, true);
                }
            }
            write_cache.try_seal();
            task.flush_to_l1().await.unwrap();
        }

        // L1 → L2
        task.merge_l1_to_l2().await.unwrap();
        assert!(segment_manager.segments_at_level(1).is_empty());

        // Create more to get ≥ 2 L2 segments
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
                write_cache.put_direct(event, true);
            }
            write_cache.try_seal();
            task.flush_to_l1().await.unwrap();
        }
        task.merge_l1_to_l2().await.unwrap();

        let l2_count = segment_manager.segments_at_level(2).len();
        assert!(l2_count >= 2, "expected >= 2 L2 segments, got {}", l2_count);

        // L2 → L3
        task.split_l2_to_l3().await.unwrap();

        assert_eq!(segment_manager.segments_at_level(2).len(), 0);
        assert_eq!(segment_manager.segments_at_level(3).len(), 3);

        for timeline in 1..=3i64 {
            let entries = index.scan_index(timeline, 0, 100);
            assert!(!entries.is_empty(), "timeline {} should have entries", timeline);
            for (_, entry) in &entries {
                let event = segment_manager.read_event(entry).unwrap();
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
        let (write_cache, segment_manager, index, task) = setup_test(dir.path(), 2, 2);

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
                write_cache.put_direct(event, true);
            }
            write_cache.try_seal();
            task.compact().await.unwrap();
        }

        // After enough compactions, should have L3 segments (one per timeline)
        let l3 = segment_manager.segments_at_level(3);
        assert_eq!(l3.len(), 2, "expected 2 L3 segments (one per timeline)");

        for timeline in 1..=2i64 {
            let entries = index.scan_index(timeline, 0, 10);
            assert_eq!(entries.len(), 4);
            for (_, entry) in &entries {
                let event = segment_manager.read_event(entry).unwrap();
                assert_eq!(event.timeline_id, timeline);
            }
        }
    }
}
