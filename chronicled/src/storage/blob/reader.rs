use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;

use chronicle_proto::pb_ext::Event;
use prost::Message;

use crate::error::unit_error::UnitError;
use crate::segment::Segment;

use super::ENTRY_HEADER_SIZE;

pub struct BlobReader {
    inner: BlobReaderInner,
}

enum BlobReaderInner {
    File(File),
    Segment(Box<dyn Segment + Sync>),
}

impl BlobReader {
    pub fn open(path: &Path) -> Result<Self, UnitError> {
        let file = File::open(path)
            .map_err(|e| UnitError::Storage(format!("failed to open segment file: {}", e)))?;
        Ok(Self {
            inner: BlobReaderInner::File(file),
        })
    }

    pub fn from_segment(segment: Box<dyn Segment + Sync>) -> Self {
        Self {
            inner: BlobReaderInner::Segment(segment),
        }
    }

    pub fn read_event(&self, byte_offset: u64, length: u32) -> Result<Event, UnitError> {
        let len = length as usize;
        if len <= ENTRY_HEADER_SIZE {
            return Err(UnitError::Storage("segment entry too small".into()));
        }
        let mut buf = vec![0u8; len];

        match &self.inner {
            BlobReaderInner::File(file) => {
                file.read_at(&mut buf, byte_offset)
                    .map_err(|e| UnitError::Storage(format!("segment pread failed: {}", e)))?;
            }
            BlobReaderInner::Segment(segment) => {
                segment.read_at(&mut buf, byte_offset)
                    .map_err(|e| UnitError::Storage(format!("segment read_at failed: {}", e)))?;
            }
        }

        let proto_data = &buf[ENTRY_HEADER_SIZE..];
        Event::decode(proto_data)
            .map_err(|e| UnitError::Storage(format!("failed to decode event from segment: {}", e)))
    }
}
