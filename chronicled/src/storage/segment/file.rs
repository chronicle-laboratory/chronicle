use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Mutex;

use chronicle_proto::pb_ext::Event;
use prost::Message;

use crate::error::unit_error::UnitError;

const ENTRY_HEADER_SIZE: usize = 20;

pub struct SegmentFileWriter {
    writer: BufWriter<File>,
    position: u64,
    segment_id: u64,
}

impl SegmentFileWriter {
    pub fn new(path: &Path, segment_id: u64) -> Result<Self, UnitError> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .map_err(|e| UnitError::Storage(format!("failed to create segment file: {}", e)))?;

        Ok(Self {
            writer: BufWriter::new(file),
            position: 0,
            segment_id,
        })
    }

    pub fn write_entry(&mut self, event: &Event) -> Result<(u64, u32), UnitError> {
        let proto_data = event.encode_to_vec();
        let payload_len = proto_data.len() as u32;

        let entry_start = self.position;
        let total_len = ENTRY_HEADER_SIZE as u32 + payload_len;

        self.writer
            .write_all(&event.timeline_id.to_be_bytes())
            .map_err(|e| UnitError::Storage(e.to_string()))?;
        self.writer
            .write_all(&event.offset.to_be_bytes())
            .map_err(|e| UnitError::Storage(e.to_string()))?;
        self.writer
            .write_all(&payload_len.to_le_bytes())
            .map_err(|e| UnitError::Storage(e.to_string()))?;

        self.writer
            .write_all(&proto_data)
            .map_err(|e| UnitError::Storage(e.to_string()))?;

        self.position += total_len as u64;

        Ok((entry_start, total_len))
    }

    pub fn finish(self) -> Result<(), UnitError> {
        let file = self
            .writer
            .into_inner()
            .map_err(|e| UnitError::Storage(format!("failed to flush segment writer: {}", e)))?;
        file.sync_all()
            .map_err(|e| UnitError::Storage(format!("failed to fsync segment file: {}", e)))?;
        Ok(())
    }

    pub fn segment_id(&self) -> u64 {
        self.segment_id
    }
}

pub struct SegmentFileReader {
    file: Mutex<File>,
}

impl SegmentFileReader {
    pub fn open(path: &Path) -> Result<Self, UnitError> {
        let file = File::open(path)
            .map_err(|e| UnitError::Storage(format!("failed to open segment file: {}", e)))?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }

    pub fn read_event(&self, byte_offset: u64, length: u32) -> Result<Event, UnitError> {
        let len = length as usize;
        if len <= ENTRY_HEADER_SIZE {
            return Err(UnitError::Storage("segment entry too small".into()));
        }
        let mut buf = vec![0u8; len];
        let file = self.file.lock().unwrap();
        file.read_at(&mut buf, byte_offset)
            .map_err(|e| UnitError::Storage(format!("segment pread failed: {}", e)))?;
        drop(file);

        let proto_data = &buf[ENTRY_HEADER_SIZE..];
        Event::decode(proto_data)
            .map_err(|e| UnitError::Storage(format!("failed to decode event from segment: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_write_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("segment_000001.cseg");

        let event = Event {
            timeline_id: 1,
            term: 1,
            offset: 0,
            payload: Some(b"hello".to_vec().into()),
            crc32: None,
            timestamp: 100,
        };

        let (byte_offset, length) = {
            let mut writer = SegmentFileWriter::new(&path, 1).unwrap();
            let result = writer.write_entry(&event).unwrap();
            writer.finish().unwrap();
            result
        };

        let reader = SegmentFileReader::open(&path).unwrap();
        let read_event = reader.read_event(byte_offset, length).unwrap();
        assert_eq!(read_event.timeline_id, 1);
        assert_eq!(read_event.offset, 0);
        assert_eq!(read_event.payload, Some(b"hello".to_vec().into()));
        assert_eq!(read_event.timestamp, 100);
    }

    #[test]
    fn test_segment_multiple_entries() {
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
                    payload: Some(format!("event_{}", i).into_bytes().into()),
                    crc32: None,
                    timestamp: i * 100,
                };
                let (offset, len) = writer.write_entry(&event).unwrap();
                entries.push((offset, len, i));
            }
            writer.finish().unwrap();
        }

        let reader = SegmentFileReader::open(&path).unwrap();
        for (byte_offset, length, expected_offset) in entries {
            let event = reader.read_event(byte_offset, length).unwrap();
            assert_eq!(event.offset, expected_offset);
            assert_eq!(event.timeline_id, 1);
        }
    }
}
