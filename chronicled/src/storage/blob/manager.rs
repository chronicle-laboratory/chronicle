use std::collections::HashMap;
use std::fs;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use chronicle_proto::pb_ext::Event;
use lru::LruCache;

use crate::error::unit_error::UnitError;
use crate::option::unit_options::IoMode;
use crate::segment::Segment;
use crate::segment::direct::DirectSegment;
use crate::segment::mmap::MmapSegment;
use crate::segment::remote::RemoteSegment;
use crate::segment::standard::StandardSegment;
use super::{BlobReader, BlobWriter};
use super::remote::RemoteStore;
use crate::storage::index::IndexEntry;

#[derive(Debug, Clone)]
pub enum SegmentLocation {
    Local,
    Remote { key: String },
}

#[derive(Debug, Clone)]
pub struct SegmentMeta {
    pub id: u64,
    pub level: u32,
    pub size: u64,
    pub entry_count: u64,
    pub location: SegmentLocation,
}

pub struct SegmentManager {
    segments_dir: PathBuf,
    readers: RwLock<HashMap<u64, BlobReader>>,
    meta: RwLock<HashMap<u64, SegmentMeta>>,
    next_segment_id: AtomicU64,
    io_mode: IoMode,
    remote_cache: Mutex<LruCache<u64, BlobReader>>,
    remote_store: Option<Arc<dyn RemoteStore>>,
}

/// Parse a segment filename like `L0_000001.cseg` into (level, id).
/// Also supports legacy `segment_000001.cseg` format (treated as level 0).
fn parse_segment_filename(name: &str) -> Option<(u32, u64)> {
    if let Some(rest) = name.strip_suffix(".cseg") {
        if let Some(rest) = rest.strip_prefix("L") {
            let parts: Vec<&str> = rest.splitn(2, '_').collect();
            if parts.len() == 2 {
                let level = parts[0].parse::<u32>().ok()?;
                let id = parts[1].parse::<u64>().ok()?;
                return Some((level, id));
            }
        }
        // Legacy format: segment_000001.cseg
        if let Some(id_str) = rest.strip_prefix("segment_") {
            let id = id_str.parse::<u64>().ok()?;
            return Some((0, id));
        }
    }
    None
}

fn segment_filename(level: u32, id: u64) -> String {
    format!("L{}_{:06}.cseg", level, id)
}

const DEFAULT_REMOTE_CACHE_SIZE: usize = 64;

impl SegmentManager {
    pub fn recover(segments_dir: PathBuf, io_mode: IoMode) -> Result<Self, UnitError> {
        Self::recover_with_remote(segments_dir, io_mode, None, DEFAULT_REMOTE_CACHE_SIZE)
    }

    pub fn recover_with_remote(
        segments_dir: PathBuf,
        io_mode: IoMode,
        remote_store: Option<Arc<dyn RemoteStore>>,
        remote_cache_capacity: usize,
    ) -> Result<Self, UnitError> {
        fs::create_dir_all(&segments_dir).map_err(|e| {
            UnitError::Storage(format!("failed to create segments dir: {}", e))
        })?;

        let mut max_id = 0u64;
        let mut meta_map = HashMap::new();

        for entry in fs::read_dir(&segments_dir)
            .map_err(|e| UnitError::Storage(format!("failed to read segments dir: {}", e)))?
        {
            let entry = entry.map_err(|e| UnitError::Storage(e.to_string()))?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            if let Some((level, id)) = parse_segment_filename(&name_str) {
                let file_size = entry.metadata()
                    .map(|m| m.len())
                    .unwrap_or(0);

                meta_map.insert(id, SegmentMeta {
                    id,
                    level,
                    size: file_size,
                    entry_count: 0,
                    location: SegmentLocation::Local,
                });

                max_id = max_id.max(id + 1);
            }
        }

        let cap = NonZeroUsize::new(remote_cache_capacity.max(1)).unwrap();

        Ok(Self {
            segments_dir,
            readers: RwLock::new(HashMap::new()),
            meta: RwLock::new(meta_map),
            next_segment_id: AtomicU64::new(max_id),
            io_mode,
            remote_cache: Mutex::new(LruCache::new(cap)),
            remote_store,
        })
    }

    pub fn set_remote_store(&mut self, remote_store: Arc<dyn RemoteStore>) {
        self.remote_store = Some(remote_store);
    }

    pub async fn new_writer_at_level(&self, level: u32) -> Result<BlobWriter, UnitError> {
        let id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        let path = self.segments_dir.join(segment_filename(level, id));

        let segment: Box<dyn Segment> = match self.io_mode {
            IoMode::Advanced => {
                let ds = DirectSegment::new(path)
                    .await
                    .map_err(|e| UnitError::Storage(e.to_string()))?;
                Box::new(ds)
            }
            IoMode::Basic => {
                let s = StandardSegment::new(path)
                    .await
                    .map_err(|e| UnitError::Storage(e.to_string()))?;
                Box::new(s)
            }
            IoMode::Mmap => {
                let ms = MmapSegment::new(path)
                    .await
                    .map_err(|e| UnitError::Storage(e.to_string()))?;
                Box::new(ms)
            }
        };

        self.meta.write().unwrap().insert(id, SegmentMeta {
            id,
            level,
            size: 0,
            entry_count: 0,
            location: SegmentLocation::Local,
        });

        Ok(BlobWriter::new(segment, id))
    }

    pub async fn new_writer(&self) -> Result<BlobWriter, UnitError> {
        self.new_writer_at_level(0).await
    }

    pub fn update_meta(&self, id: u64, size: u64, entry_count: u64) {
        if let Some(meta) = self.meta.write().unwrap().get_mut(&id) {
            meta.size = size;
            meta.entry_count = entry_count;
        }
    }

    pub fn segments_at_level(&self, level: u32) -> Vec<SegmentMeta> {
        self.meta.read().unwrap()
            .values()
            .filter(|m| m.level == level && matches!(m.location, SegmentLocation::Local))
            .cloned()
            .collect()
    }

    pub fn remove_segments(&self, ids: &[u64]) {
        let mut readers = self.readers.write().unwrap();
        let mut meta = self.meta.write().unwrap();

        for &id in ids {
            readers.remove(&id);
            if let Some(m) = meta.remove(&id) {
                if matches!(m.location, SegmentLocation::Local) {
                    let path = self.segments_dir.join(segment_filename(m.level, id));
                    let _ = fs::remove_file(&path);
                    // Also try legacy filename
                    let legacy = self.segments_dir.join(format!("segment_{:06}.cseg", id));
                    let _ = fs::remove_file(&legacy);
                }
            }
        }
    }

    /// Mark a segment as offloaded to remote storage.
    /// Removes the local file and reader, but keeps meta with Remote location.
    pub fn mark_remote(&self, id: u64, key: String) {
        let mut readers = self.readers.write().unwrap();
        let mut meta = self.meta.write().unwrap();

        readers.remove(&id);

        if let Some(m) = meta.get_mut(&id) {
            // Delete local file
            let path = self.segments_dir.join(segment_filename(m.level, id));
            let _ = fs::remove_file(&path);

            m.location = SegmentLocation::Remote { key };
        }
    }

    /// Remove segment files that are not referenced in the given set of known IDs.
    pub fn cleanup_orphans(&self, referenced_ids: &std::collections::HashSet<u64>) {
        let meta = self.meta.read().unwrap();
        let orphan_ids: Vec<u64> = meta.keys()
            .filter(|id| !referenced_ids.contains(id))
            .copied()
            .collect();
        drop(meta);

        if !orphan_ids.is_empty() {
            self.remove_segments(&orphan_ids);
        }
    }

    pub fn segment_path_for(&self, id: u64) -> Option<PathBuf> {
        let meta = self.meta.read().unwrap();
        meta.get(&id).and_then(|m| {
            if matches!(m.location, SegmentLocation::Local) {
                Some(self.segments_dir.join(segment_filename(m.level, id)))
            } else {
                None
            }
        })
    }

    pub fn segment_meta(&self, id: u64) -> Option<SegmentMeta> {
        self.meta.read().unwrap().get(&id).cloned()
    }

    pub fn read_event(&self, entry: &IndexEntry) -> Result<Event, UnitError> {
        // Try local reader cache first
        {
            let readers = self.readers.read().unwrap();
            if let Some(reader) = readers.get(&entry.segment_id) {
                return reader.read_event(entry.byte_offset, entry.length);
            }
        }

        // Check meta for location
        let location = {
            let meta = self.meta.read().unwrap();
            meta.get(&entry.segment_id).map(|m| m.location.clone())
        };

        match location {
            Some(SegmentLocation::Local) => {
                let path = self.find_local_path(entry.segment_id)?;
                let reader = BlobReader::open(&path)?;
                let event = reader.read_event(entry.byte_offset, entry.length)?;
                self.readers.write().unwrap().insert(entry.segment_id, reader);
                Ok(event)
            }
            Some(SegmentLocation::Remote { .. }) => {
                // Try LRU cache for downloaded remote segments
                let mut cache = self.remote_cache.lock().unwrap();
                if let Some(reader) = cache.get(&entry.segment_id) {
                    return reader.read_event(entry.byte_offset, entry.length);
                }

                Err(UnitError::Storage(format!(
                    "segment {} is remote; use read_event_async for remote reads",
                    entry.segment_id,
                )))
            }
            None => {
                // No meta — try legacy local path
                let path = self.find_local_path(entry.segment_id)?;
                let reader = BlobReader::open(&path)?;
                let event = reader.read_event(entry.byte_offset, entry.length)?;
                self.readers.write().unwrap().insert(entry.segment_id, reader);
                Ok(event)
            }
        }
    }

    /// Read an event, downloading from remote storage if needed.
    pub async fn read_event_async(&self, entry: &IndexEntry) -> Result<Event, UnitError> {
        // Try sync path first (local readers + local files)
        match self.read_event(entry) {
            Ok(event) => return Ok(event),
            Err(UnitError::Storage(msg)) if msg.contains("is remote") => {}
            Err(e) => return Err(e),
        }

        // Fetch from remote
        let remote_key = {
            let meta = self.meta.read().unwrap();
            match meta.get(&entry.segment_id) {
                Some(SegmentMeta { location: SegmentLocation::Remote { key }, .. }) => key.clone(),
                _ => return Err(UnitError::Storage(format!(
                    "segment {} not found", entry.segment_id
                ))),
            }
        };

        let remote_store = self.remote_store.as_ref().ok_or_else(|| {
            UnitError::Storage("no remote store configured".into())
        })?;

        // Download whole segment into memory
        let data = remote_store.download(&remote_key).await?;
        let segment = RemoteSegment::from_bytes(data);
        let reader = BlobReader::from_segment(Box::new(segment));
        let event = reader.read_event(entry.byte_offset, entry.length)?;

        // Cache in LRU
        self.remote_cache.lock().unwrap().push(entry.segment_id, reader);

        Ok(event)
    }

    fn find_local_path(&self, id: u64) -> Result<PathBuf, UnitError> {
        if let Some(path) = self.segment_path_for(id) {
            if path.exists() {
                return Ok(path);
            }
        }
        let legacy = self.segments_dir.join(format!("segment_{:06}.cseg", id));
        if legacy.exists() {
            return Ok(legacy);
        }
        Err(UnitError::Storage(format!("segment file not found for id {}", id)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_segment_filename() {
        assert_eq!(parse_segment_filename("L0_000001.cseg"), Some((0, 1)));
        assert_eq!(parse_segment_filename("L1_000042.cseg"), Some((1, 42)));
        assert_eq!(parse_segment_filename("L2_000100.cseg"), Some((2, 100)));
        assert_eq!(parse_segment_filename("segment_000003.cseg"), Some((0, 3)));
        assert_eq!(parse_segment_filename("garbage.txt"), None);
    }

    #[test]
    fn test_segment_filename_format() {
        assert_eq!(segment_filename(0, 1), "L0_000001.cseg");
        assert_eq!(segment_filename(2, 42), "L2_000042.cseg");
    }

    #[test]
    fn test_segment_manager_recover_empty() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = SegmentManager::recover(dir.path().to_path_buf(), IoMode::Basic).unwrap();
        assert_eq!(mgr.next_segment_id.load(Ordering::Relaxed), 0);
        assert!(mgr.segments_at_level(0).is_empty());
    }

    #[tokio::test]
    async fn test_segment_manager_write_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = SegmentManager::recover(dir.path().to_path_buf(), IoMode::Basic).unwrap();

        let event = Event {
            timeline_id: 5,
            term: 1,
            offset: 42,
            payload: Some(b"test_payload".to_vec().into()),
            crc32: None,
            timestamp: 999,
        };

        let mut writer = mgr.new_writer().await.unwrap();
        let seg_id = writer.segment_id();
        let (byte_offset, length) = writer.write_entry(&event).await.unwrap();
        writer.finish().await.unwrap();

        let entry = IndexEntry {
            segment_id: seg_id,
            byte_offset,
            length,
        };

        let read_event = mgr.read_event(&entry).unwrap();
        assert_eq!(read_event.timeline_id, 5);
        assert_eq!(read_event.offset, 42);
        assert_eq!(read_event.payload, Some(b"test_payload".to_vec().into()));
    }

    #[tokio::test]
    async fn test_segment_manager_recover_existing() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mgr = SegmentManager::recover(dir.path().to_path_buf(), IoMode::Basic).unwrap();
            let writer = mgr.new_writer().await.unwrap();
            writer.finish().await.unwrap();
            let writer = mgr.new_writer().await.unwrap();
            writer.finish().await.unwrap();
        }

        let mgr = SegmentManager::recover(dir.path().to_path_buf(), IoMode::Basic).unwrap();
        assert_eq!(mgr.next_segment_id.load(Ordering::Relaxed), 2);
        assert_eq!(mgr.segments_at_level(0).len(), 2);
    }

    #[tokio::test]
    async fn test_segment_manager_level_aware_writer() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = SegmentManager::recover(dir.path().to_path_buf(), IoMode::Basic).unwrap();

        let writer = mgr.new_writer_at_level(1).await.unwrap();
        let id = writer.segment_id();
        writer.finish().await.unwrap();

        assert_eq!(mgr.segments_at_level(1).len(), 1);
        assert_eq!(mgr.segments_at_level(0).len(), 0);

        let path = mgr.segment_path_for(id).unwrap();
        assert!(path.file_name().unwrap().to_string_lossy().starts_with("L1_"));
    }

    #[tokio::test]
    async fn test_segment_manager_remove_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = SegmentManager::recover(dir.path().to_path_buf(), IoMode::Basic).unwrap();

        let w1 = mgr.new_writer().await.unwrap();
        let id1 = w1.segment_id();
        w1.finish().await.unwrap();

        let w2 = mgr.new_writer().await.unwrap();
        let id2 = w2.segment_id();
        w2.finish().await.unwrap();

        assert_eq!(mgr.segments_at_level(0).len(), 2);

        mgr.remove_segments(&[id1]);
        assert_eq!(mgr.segments_at_level(0).len(), 1);
        assert!(mgr.segment_path_for(id1).is_none());
        assert!(mgr.segment_path_for(id2).is_some());
    }

    #[tokio::test]
    async fn test_segment_manager_mark_remote() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = SegmentManager::recover(dir.path().to_path_buf(), IoMode::Basic).unwrap();

        let event = Event {
            timeline_id: 1,
            term: 1,
            offset: 0,
            payload: Some(b"data".to_vec().into()),
            crc32: None,
            timestamp: 100,
        };

        let mut writer = mgr.new_writer_at_level(2).await.unwrap();
        let seg_id = writer.segment_id();
        writer.write_entry(&event).await.unwrap();
        writer.finish().await.unwrap();

        assert_eq!(mgr.segments_at_level(2).len(), 1);
        assert!(mgr.segment_path_for(seg_id).is_some());

        mgr.mark_remote(seg_id, "chronicle/segments/L2_000000.cseg".into());

        // No longer appears in local segments
        assert_eq!(mgr.segments_at_level(2).len(), 0);
        assert!(mgr.segment_path_for(seg_id).is_none());

        // Meta still exists with Remote location
        let meta = mgr.segment_meta(seg_id).unwrap();
        assert!(matches!(meta.location, SegmentLocation::Remote { .. }));
    }

    #[tokio::test]
    async fn test_segment_manager_mmap_write_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = SegmentManager::recover(dir.path().to_path_buf(), IoMode::Mmap).unwrap();

        let event = Event {
            timeline_id: 7,
            term: 2,
            offset: 99,
            payload: Some(b"mmap_test".to_vec().into()),
            crc32: None,
            timestamp: 555,
        };

        let mut writer = mgr.new_writer().await.unwrap();
        let seg_id = writer.segment_id();
        let (byte_offset, length) = writer.write_entry(&event).await.unwrap();
        writer.finish().await.unwrap();

        let entry = IndexEntry {
            segment_id: seg_id,
            byte_offset,
            length,
        };

        let read_event = mgr.read_event(&entry).unwrap();
        assert_eq!(read_event.timeline_id, 7);
        assert_eq!(read_event.offset, 99);
        assert_eq!(read_event.payload, Some(b"mmap_test".to_vec().into()));
    }

    #[tokio::test]
    async fn test_remote_cache_lru_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = SegmentManager::recover_with_remote(
            dir.path().to_path_buf(),
            IoMode::Basic,
            None,
            2, // tiny LRU: capacity 2
        ).unwrap();

        // Simulate cached remote segments
        for i in 0..3u64 {
            let segment = RemoteSegment::from_bytes(vec![0u8; 10]);
            let reader = BlobReader::from_segment(Box::new(segment));

            let mut cache = mgr.remote_cache.lock().unwrap();
            cache.push(i, reader);
        }

        let cache = mgr.remote_cache.lock().unwrap();
        // Only 2 entries should remain (LRU capacity)
        assert_eq!(cache.len(), 2);
        // Entry 0 should have been evicted
        assert!(!cache.contains(&0));
        assert!(cache.contains(&1));
        assert!(cache.contains(&2));
    }
}
