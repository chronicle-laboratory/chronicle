// This is a standalone test for WAL record encoding/decoding
// It tests the WAL record format without depending on the rest of the codebase

use std::io::{Error, ErrorKind};

// Inline the record implementation for testing
const LENGTH_SIZE: usize = 4;
const CRC_SIZE: usize = 4;
const RECORD_HEADER_SIZE: usize = LENGTH_SIZE + CRC_SIZE;

#[derive(Debug)]
struct Record {
    data: Vec<u8>,
}

impl Record {
    fn new(data: Vec<u8>) -> Self {
        Record { data }
    }

    fn encode(&self) -> Vec<u8> {
        let len = self.data.len() as u32;
        let crc = xxh32(&self.data, 0);

        let mut encoded = Vec::with_capacity(RECORD_HEADER_SIZE + self.data.len());
        encoded.extend_from_slice(&len.to_le_bytes());
        encoded.extend_from_slice(&crc.to_le_bytes());
        encoded.extend_from_slice(&self.data);
        encoded
    }

    fn decode(bytes: &[u8]) -> Result<(Self, usize), Error> {
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

// Simple XXH32 implementation (inline for testing)
fn xxh32(data: &[u8], seed: u32) -> u32 {
    // Use the xxhash-rust crate's implementation
    use xxhash_rust::xxh32::xxh32 as xxh32_impl;
    xxh32_impl(data, seed)
}

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
fn test_multiple_records() {
    let records = vec![
        Record::new(b"record1".to_vec()),
        Record::new(b"record2".to_vec()),
        Record::new(b"record3".to_vec()),
    ];

    let mut encoded = Vec::new();
    for record in &records {
        encoded.extend_from_slice(&record.encode());
    }

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

#[test]
fn test_empty_record() {
    let data = Vec::new();
    let record = Record::new(data);
    let encoded = record.encode();

    let (decoded, _size) = Record::decode(&encoded).unwrap();
    assert_eq!(decoded.data.len(), 0);
}

#[test]
fn test_large_record() {
    let data = vec![0xAB; 1024 * 1024]; // 1MB of data
    let record = Record::new(data.clone());
    let encoded = record.encode();

    let (decoded, _size) = Record::decode(&encoded).unwrap();
    assert_eq!(decoded.data, data);
}
