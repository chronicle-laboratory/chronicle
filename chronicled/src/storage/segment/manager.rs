use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use chronicle_proto::pb_ext::Event;

use crate::error::unit_error::UnitError;
use crate::option::unit_options::IoMode;
use crate::storage::segment::file::{SegmentFileReader, SegmentFileWriter};
use crate::storage::segment::mmap_reader::MmapSegmentReader;
use crate::storage::index::IndexEntry;

enum SegmentReader {
    Basic(SegmentFileReader),
    Mmap(MmapSegmentReader),
}

impl SegmentReader {
    fn read_event(&self, byte_offset: u64, length: u32) -> Result<Event, UnitError> {
        match self {
            SegmentReader::Basic(r) => r.read_event(byte_offset, length),
            SegmentReader::Mmap(r) => r.read_event(byte_offset, length),
        }
    }
}

pub struct SegmentManager {
    segments_dir: PathBuf,
    readers: RwLock<HashMap<u64, SegmentReader>>,
    next_segment_id: AtomicU64,
    io_mode: IoMode,
}

impl SegmentManager {
    pub fn recover(segments_dir: PathBuf, io_mode: IoMode) -> Result<Self, UnitError> {
        fs::create_dir_all(&segments_dir).map_err(|e| {
            UnitError::Storage(format!("failed to create segments dir: {}", e))
        })?;

        let mut max_id = 0u64;
        for entry in fs::read_dir(&segments_dir)
            .map_err(|e| UnitError::Storage(format!("failed to read segments dir: {}", e)))?
        {
            let entry = entry.map_err(|e| UnitError::Storage(e.to_string()))?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(id_str) = name_str
                .strip_prefix("segment_")
                .and_then(|s| s.strip_suffix(".cseg"))
            {
                if let Ok(id) = id_str.parse::<u64>() {
                    max_id = max_id.max(id + 1);
                }
            }
        }

        Ok(Self {
            segments_dir,
            readers: RwLock::new(HashMap::new()),
            next_segment_id: AtomicU64::new(max_id),
            io_mode,
        })
    }

    pub fn new_writer(&self) -> Result<SegmentFileWriter, UnitError> {
        let id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        let path = self.segment_path(id);
        SegmentFileWriter::new(&path, id)
    }

    pub fn read_event(&self, entry: &IndexEntry) -> Result<Event, UnitError> {
        {
            let readers = self.readers.read().unwrap();
            if let Some(reader) = readers.get(&entry.segment_id) {
                return reader.read_event(entry.byte_offset, entry.length);
            }
        }

        let path = self.segment_path(entry.segment_id);
        let reader = self.open_reader(&path)?;
        let event = reader.read_event(entry.byte_offset, entry.length)?;
        self.readers
            .write()
            .unwrap()
            .insert(entry.segment_id, reader);
        Ok(event)
    }

    fn open_reader(&self, path: &PathBuf) -> Result<SegmentReader, UnitError> {
        match self.io_mode {
            IoMode::Advanced => MmapSegmentReader::open(path).map(SegmentReader::Mmap),
            IoMode::Basic => SegmentFileReader::open(path).map(SegmentReader::Basic),
        }
    }

    fn segment_path(&self, id: u64) -> PathBuf {
        self.segments_dir.join(format!("segment_{:06}.cseg", id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_manager_recover_empty() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = SegmentManager::recover(dir.path().to_path_buf(), IoMode::Basic).unwrap();
        assert_eq!(mgr.next_segment_id.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_segment_manager_write_and_read() {
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

        let mut writer = mgr.new_writer().unwrap();
        let seg_id = writer.segment_id();
        let (byte_offset, length) = writer.write_entry(&event).unwrap();
        writer.finish().unwrap();

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

    #[test]
    fn test_segment_manager_recover_existing() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mgr = SegmentManager::recover(dir.path().to_path_buf(), IoMode::Basic).unwrap();
            let _ = mgr.new_writer().unwrap();
            let _ = mgr.new_writer().unwrap();
        }

        let mgr = SegmentManager::recover(dir.path().to_path_buf(), IoMode::Basic).unwrap();
        assert_eq!(mgr.next_segment_id.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_segment_manager_mmap_read() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = SegmentManager::recover(dir.path().to_path_buf(), IoMode::Advanced).unwrap();

        let event = Event {
            timeline_id: 7,
            term: 2,
            offset: 99,
            payload: Some(b"mmap_test".to_vec().into()),
            crc32: None,
            timestamp: 555,
        };

        let mut writer = mgr.new_writer().unwrap();
        let seg_id = writer.segment_id();
        let (byte_offset, length) = writer.write_entry(&event).unwrap();
        writer.finish().unwrap();

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
}
