// This is a standalone test for WAL record encoding/decoding
// It tests the RocksDB-compatible WAL record format

use std::io::{Error, ErrorKind};

// RocksDB-compatible WAL Record Format:
// +----------+-----------+-----------+--- ... ---+
// |CRC (4B)  | Size (2B) | Type (1B) | Payload   |
// +----------+-----------+-----------+--- ... ---+

const CRC_SIZE: usize = 4;
const SIZE_FIELD_SIZE: usize = 2;
const TYPE_SIZE: usize = 1;
const RECORD_HEADER_SIZE: usize = CRC_SIZE + SIZE_FIELD_SIZE + TYPE_SIZE;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecordType {
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

impl RecordType {
    fn from_u8(value: u8) -> Result<Self, Error> {
        match value {
            1 => Ok(RecordType::Full),
            2 => Ok(RecordType::First),
            3 => Ok(RecordType::Middle),
            4 => Ok(RecordType::Last),
            _ => Err(Error::new(
                ErrorKind::InvalidData,
                format!("invalid record type: {}", value),
            )),
        }
    }
}

#[derive(Debug)]
struct Record {
    record_type: RecordType,
    data: Vec<u8>,
}

impl Record {
    fn new(data: Vec<u8>) -> Self {
        Record {
            record_type: RecordType::Full,
            data,
        }
    }

    fn new_with_type(record_type: RecordType, data: Vec<u8>) -> Self {
        Record { record_type, data }
    }

    fn encode(&self) -> Vec<u8> {
        let size = self.data.len() as u16;
        let record_type = self.record_type as u8;
        
        // Compute CRC over type and payload (RocksDB compatible)
        let mut crc_data = Vec::with_capacity(1 + self.data.len());
        crc_data.push(record_type);
        crc_data.extend_from_slice(&self.data);
        let crc = xxh32(&crc_data, 0);

        let mut encoded = Vec::with_capacity(RECORD_HEADER_SIZE + self.data.len());
        encoded.extend_from_slice(&crc.to_le_bytes());
        encoded.extend_from_slice(&size.to_le_bytes());
        encoded.push(record_type);
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

        let expected_crc = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let size = u16::from_le_bytes([bytes[4], bytes[5]]) as usize;
        let record_type_byte = bytes[6];
        
        let record_type = RecordType::from_u8(record_type_byte)?;

        let total_size = RECORD_HEADER_SIZE + size;
        if bytes.len() < total_size {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                format!("insufficient bytes for record data: expected {}, got {}", total_size, bytes.len()),
            ));
        }

        let data = &bytes[RECORD_HEADER_SIZE..total_size];
        
        // Compute CRC over type and payload
        let mut crc_data = Vec::with_capacity(1 + size);
        crc_data.push(record_type_byte);
        crc_data.extend_from_slice(data);
        let actual_crc = xxh32(&crc_data, 0);

        if actual_crc != expected_crc {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("checksum mismatch: expected {}, got {}", expected_crc, actual_crc),
            ));
        }

        Ok((Record { record_type, data: data.to_vec() }, total_size))

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
    assert_eq!(decoded.record_type, RecordType::Full);
    assert_eq!(size, encoded.len());
}

#[test]
fn test_record_types() {
    for record_type in [RecordType::Full, RecordType::First, RecordType::Middle, RecordType::Last] {
        let data = b"test data".to_vec();
        let record = Record::new_with_type(record_type, data.clone());
        let encoded = record.encode();

        let (decoded, _) = Record::decode(&encoded).unwrap();
        assert_eq!(decoded.data, data);
        assert_eq!(decoded.record_type, record_type);
    }
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
        assert_eq!(record.record_type, RecordType::Full);
        offset += size;
    }
}

#[test]
fn test_crc_computed_over_type_and_data() {
    let data = b"test".to_vec();
    let record = Record::new(data.clone());
    let encoded = record.encode();

    // Manually verify CRC is computed over type + data
    let mut crc_data = vec![RecordType::Full as u8];
    crc_data.extend_from_slice(&data);
    let expected_crc = xxh32(&crc_data, 0);
    let encoded_crc = u32::from_le_bytes([encoded[0], encoded[1], encoded[2], encoded[3]]);
    
    assert_eq!(encoded_crc, expected_crc);
}

#[test]
fn test_header_size() {
    // Verify header size is 7 bytes (4 CRC + 2 Size + 1 Type)
    assert_eq!(RECORD_HEADER_SIZE, 7);
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
    assert_eq!(decoded.record_type, RecordType::Full);
}

#[test]
#[should_panic(expected = "Record data too large")]
fn test_large_record() {
    // RocksDB format uses u16 for size, max 65535 bytes
    let data = vec![0xAB; 70000]; // Exceeds u16::MAX
    let record = Record::new(data);
    let _encoded = record.encode(); // Should panic
}

#[test]
fn test_max_size_record() {
    // Test maximum valid size (u16::MAX = 65535 bytes)
    let data = vec![0xAB; 65535];
    let record = Record::new(data.clone());
    let encoded = record.encode();

    let (decoded, _size) = Record::decode(&encoded).unwrap();
    assert_eq!(decoded.data, data);
}
