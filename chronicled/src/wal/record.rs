use std::io::{Error, ErrorKind};
use xxhash_rust::xxh32::xxh32;

// WAL Record Format:
// +------------------+------------------+------------------+
// | Length (4 bytes) | CRC32 (4 bytes)  | Data (N bytes)   |
// +------------------+------------------+------------------+
//
// Length: length of data in bytes (little-endian u32)
// CRC32: XXH32 checksum of data (little-endian u32)
// Data: actual record payload

const LENGTH_SIZE: usize = 4;
const CRC_SIZE: usize = 4;
pub const RECORD_HEADER_SIZE: usize = LENGTH_SIZE + CRC_SIZE;

/// Represents a WAL record with length, checksum, and data
#[derive(Debug)]
pub struct Record {
    pub data: Vec<u8>,
}

impl Record {
    /// Create a new record from data
    pub fn new(data: Vec<u8>) -> Self {
        Record { data }
    }

    /// Encode the record into bytes with header
    pub fn encode(&self) -> Vec<u8> {
        let len = self.data.len() as u32;
        let crc = xxh32(&self.data, 0);

        let mut encoded = Vec::with_capacity(RECORD_HEADER_SIZE + self.data.len());
        encoded.extend_from_slice(&len.to_le_bytes());
        encoded.extend_from_slice(&crc.to_le_bytes());
        encoded.extend_from_slice(&self.data);
        encoded
    }

    /// Decode a record from bytes
    pub fn decode(bytes: &[u8]) -> Result<(Self, usize), Error> {
        if bytes.len() < RECORD_HEADER_SIZE {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "insufficient bytes for record header",
            ));
        }

        let len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        let expected_crc = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);

        let total_size = RECORD_HEADER_SIZE + len;
        if bytes.len() < total_size {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!("insufficient bytes for record data: expected {}, got {}", total_size, bytes.len()),
            ));
        }

        let data = &bytes[RECORD_HEADER_SIZE..total_size];
        let actual_crc = xxh32(data, 0);

        if actual_crc != expected_crc {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("checksum mismatch: expected {}, got {}", expected_crc, actual_crc),
            ));
        }

        Ok((Record { data: data.to_vec() }, total_size))
    }
}

/// Batch of records for group writes
pub struct RecordBatch {
    pub records: Vec<Record>,
}

impl RecordBatch {
    pub fn new() -> Self {
        RecordBatch {
            records: Vec::new(),
        }
    }

    pub fn add(&mut self, record: Record) {
        self.records.push(record);
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        for record in &self.records {
            encoded.extend_from_slice(&record.encode());
        }
        encoded
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_encode_decode() {
        let data = b"hello world".to_vec();
        let record = Record::new(data.clone());
        let encoded = record.encode();

        let (decoded, size) = Record::decode(&encoded).unwrap();
        assert_eq!(decoded.data, data);
        assert_eq!(size, encoded.len());
    }

    #[test]
    fn test_record_checksum_validation() {
        let data = b"hello world".to_vec();
        let record = Record::new(data);
        let mut encoded = record.encode();

        // Corrupt the data
        encoded[RECORD_HEADER_SIZE] = encoded[RECORD_HEADER_SIZE].wrapping_add(1);

        let result = Record::decode(&encoded);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("checksum mismatch"));
    }

    #[test]
    fn test_record_batch() {
        let mut batch = RecordBatch::new();
        batch.add(Record::new(b"record1".to_vec()));
        batch.add(Record::new(b"record2".to_vec()));
        batch.add(Record::new(b"record3".to_vec()));

        let encoded = batch.encode();
        assert!(!encoded.is_empty());
        assert_eq!(batch.len(), 3);

        // Decode records one by one
        let mut offset = 0;
        for i in 0..3 {
            let (record, size) = Record::decode(&encoded[offset..]).unwrap();
            assert_eq!(record.data, format!("record{}", i + 1).as_bytes());
            offset += size;
        }
    }

    #[test]
    fn test_incomplete_record() {
        let data = b"hello".to_vec();
        let record = Record::new(data);
        let encoded = record.encode();

        // Try to decode with incomplete data
        let result = Record::decode(&encoded[..5]);
        assert!(result.is_err());
    }
}
