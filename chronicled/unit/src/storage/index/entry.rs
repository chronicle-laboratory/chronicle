pub fn encode_key(timeline_id: i64, offset: i64) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[..8].copy_from_slice(&timeline_id.to_be_bytes());
    key[8..].copy_from_slice(&offset.to_be_bytes());
    key
}

pub fn decode_key(key: &[u8]) -> (i64, i64) {
    let timeline_id = i64::from_be_bytes(key[..8].try_into().unwrap());
    let offset = i64::from_be_bytes(key[8..].try_into().unwrap());
    (timeline_id, offset)
}

#[derive(Debug, Clone, Copy)]
pub struct IndexEntry {
    pub segment_id: u64,
    pub byte_offset: u64,
    pub length: u32,
}

impl IndexEntry {
    pub fn encode(&self) -> [u8; 20] {
        let mut buf = [0u8; 20];
        buf[..8].copy_from_slice(&self.segment_id.to_le_bytes());
        buf[8..16].copy_from_slice(&self.byte_offset.to_le_bytes());
        buf[16..20].copy_from_slice(&self.length.to_le_bytes());
        buf
    }

    pub fn decode(data: &[u8]) -> Self {
        Self {
            segment_id: u64::from_le_bytes(data[..8].try_into().unwrap()),
            byte_offset: u64::from_le_bytes(data[8..16].try_into().unwrap()),
            length: u32::from_le_bytes(data[16..20].try_into().unwrap()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_encode_decode_roundtrip() {
        let timeline_id = 42i64;
        let offset = 100i64;
        let key = encode_key(timeline_id, offset);
        let (decoded_tid, decoded_off) = decode_key(&key);
        assert_eq!(decoded_tid, timeline_id);
        assert_eq!(decoded_off, offset);
    }

    #[test]
    fn test_key_ordering() {
        let k1 = encode_key(1, 0);
        let k2 = encode_key(1, 1);
        let k3 = encode_key(1, 100);
        assert!(k1 < k2);
        assert!(k2 < k3);

        let ka = encode_key(1, 999);
        let kb = encode_key(2, 0);
        assert!(ka < kb);
    }

    #[test]
    fn test_key_negative_values() {
        let key = encode_key(-1, -100);
        let (tid, off) = decode_key(&key);
        assert_eq!(tid, -1);
        assert_eq!(off, -100);
    }

    #[test]
    fn test_index_entry_encode_decode_roundtrip() {
        let entry = IndexEntry {
            segment_id: 42,
            byte_offset: 1024,
            length: 256,
        };
        let encoded = entry.encode();
        let decoded = IndexEntry::decode(&encoded);
        assert_eq!(decoded.segment_id, 42);
        assert_eq!(decoded.byte_offset, 1024);
        assert_eq!(decoded.length, 256);
    }
}
