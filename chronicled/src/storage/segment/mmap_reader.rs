use std::fs::File;
use std::path::Path;

use chronicle_proto::pb_ext::Event;
use memmap2::Mmap;
use prost::Message;

use crate::error::unit_error::UnitError;

const ENTRY_HEADER_SIZE: usize = 20;

pub struct MmapSegmentReader {
    mmap: Mmap,
}

impl MmapSegmentReader {
    pub fn open(path: &Path) -> Result<Self, UnitError> {
        let file = File::open(path)
            .map_err(|e| UnitError::Storage(format!("failed to open segment file: {}", e)))?;

        let mmap = unsafe {
            Mmap::map(&file)
                .map_err(|e| UnitError::Storage(format!("failed to mmap segment file: {}", e)))?
        };

        #[cfg(unix)]
        {
            use libc::{MADV_WILLNEED, madvise};
            unsafe {
                madvise(
                    mmap.as_ptr() as *mut libc::c_void,
                    mmap.len(),
                    MADV_WILLNEED,
                );
            }
        }

        Ok(Self { mmap })
    }

    pub fn read_event(&self, byte_offset: u64, length: u32) -> Result<Event, UnitError> {
        let offset = byte_offset as usize;
        let len = length as usize;

        if len <= ENTRY_HEADER_SIZE {
            return Err(UnitError::Storage("segment entry too small".into()));
        }

        let end = offset + len;
        if end > self.mmap.len() {
            return Err(UnitError::Storage(format!(
                "segment read out of bounds: offset={}, length={}, file_size={}",
                offset,
                len,
                self.mmap.len()
            )));
        }

        let proto_data = &self.mmap[offset + ENTRY_HEADER_SIZE..end];
        Event::decode(proto_data)
            .map_err(|e| UnitError::Storage(format!("failed to decode event from segment: {}", e)))
    }

    pub fn prefetch(&self, offset: usize, len: usize) {
        #[cfg(unix)]
        {
            use libc::{MADV_WILLNEED, madvise};
            let end = (offset + len).min(self.mmap.len());
            if offset < end {
                unsafe {
                    madvise(
                        self.mmap.as_ptr().add(offset) as *mut libc::c_void,
                        end - offset,
                        MADV_WILLNEED,
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::segment::file::SegmentFileWriter;

    #[test]
    fn test_mmap_reader_write_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("segment_000001.cseg");

        let event = Event {
            timeline_id: 1,
            term: 1,
            offset: 0,
            payload: Some(b"hello_mmap".to_vec().into()),
            crc32: None,
            timestamp: 100,
        };

        let (byte_offset, length) = {
            let mut writer = SegmentFileWriter::new(&path, 1).unwrap();
            let result = writer.write_entry(&event).unwrap();
            writer.finish().unwrap();
            result
        };

        let reader = MmapSegmentReader::open(&path).unwrap();
        let read_event = reader.read_event(byte_offset, length).unwrap();
        assert_eq!(read_event.timeline_id, 1);
        assert_eq!(read_event.offset, 0);
        assert_eq!(read_event.payload, Some(b"hello_mmap".to_vec().into()));
        assert_eq!(read_event.timestamp, 100);
    }

    #[test]
    fn test_mmap_reader_multiple_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("segment_000002.cseg");

        let mut entries = Vec::new();
        {
            let mut writer = SegmentFileWriter::new(&path, 2).unwrap();
            for i in 0..10 {
                let event = Event {
                    timeline_id: 1,
                    term: 1,
                    offset: i,
                    payload: Some(format!("mmap_event_{}", i).into_bytes().into()),
                    crc32: None,
                    timestamp: i * 100,
                };
                let (offset, len) = writer.write_entry(&event).unwrap();
                entries.push((offset, len, i));
            }
            writer.finish().unwrap();
        }

        let reader = MmapSegmentReader::open(&path).unwrap();
        for (byte_offset, length, expected_offset) in entries {
            let event = reader.read_event(byte_offset, length).unwrap();
            assert_eq!(event.offset, expected_offset);
            assert_eq!(event.timeline_id, 1);
        }
    }

    #[test]
    fn test_mmap_reader_bounds_check() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("segment_000003.cseg");

        let event = Event {
            timeline_id: 1,
            term: 1,
            offset: 0,
            payload: Some(b"short".to_vec().into()),
            crc32: None,
            timestamp: 100,
        };

        {
            let mut writer = SegmentFileWriter::new(&path, 3).unwrap();
            writer.write_entry(&event).unwrap();
            writer.finish().unwrap();
        }

        let reader = MmapSegmentReader::open(&path).unwrap();

        // Out of bounds read
        let result = reader.read_event(0, 99999);
        assert!(result.is_err());

        // Entry too small
        let result = reader.read_event(0, 10);
        assert!(result.is_err());
    }
}
