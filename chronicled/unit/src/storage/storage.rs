use crate::error::unit_error::UnitError;
use crate::storage::TimelineReader;
use crate::storage::write_cache::WriteCache;
use chronicle_proto::pb_ext::Event;
use futures_util::Stream;
use prost::Message;
use rocksdb::{ColumnFamilyDescriptor, DB, DBCompressionType, LogLevel, Options};
use std::sync::Arc;

pub const CF_META: &str = "meta";
pub const CF_IDX: &str = "idx";

/// Encode a (timeline_id, offset) pair into a big-endian 16-byte key.
/// Big-endian ensures correct lexicographic ordering for RocksDB iteration.
pub fn encode_key(timeline_id: i64, offset: i64) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[..8].copy_from_slice(&timeline_id.to_be_bytes());
    key[8..].copy_from_slice(&offset.to_be_bytes());
    key
}

/// Decode a big-endian 16-byte key into (timeline_id, offset).
pub fn decode_key(key: &[u8]) -> (i64, i64) {
    let timeline_id = i64::from_be_bytes(key[..8].try_into().unwrap());
    let offset = i64::from_be_bytes(key[8..].try_into().unwrap());
    (timeline_id, offset)
}

pub struct Inner {
    pub database: DB,
}

#[derive(Clone)]
pub struct Storage {
    inner: Arc<Inner>,
}

pub struct StorageOptions {
    pub path: String,
}

impl Storage {
    pub fn new(options: StorageOptions) -> Result<Storage, UnitError> {
        let mut db_options = Options::default();
        db_options.create_if_missing(true);
        db_options.create_missing_column_families(true);
        db_options.set_compression_type(DBCompressionType::Lz4);
        db_options.set_bottommost_compression_type(DBCompressionType::Zstd);
        db_options.set_log_level(LogLevel::Info);
        db_options.set_keep_log_file_num(10);

        let cf_names = vec![
            ColumnFamilyDescriptor::new(CF_META, Options::default()),
            ColumnFamilyDescriptor::new(CF_IDX, Options::default()),
        ];
        let db = DB::open_cf_descriptors(&db_options, &options.path, cf_names)
            .map_err(|err| UnitError::Storage(err.to_string()))?;

        Ok(Storage {
            inner: Arc::new(Inner { database: db }),
        })
    }

    pub(crate) fn fetch_write_cache(&self) -> WriteCache {
        WriteCache::new(self.inner.clone())
    }
}

impl TimelineReader for Storage {
    fn fetch_batches(
        &self,
        timeline_id: i64,
        start_offset: i64,
        end_offset: i64,
    ) -> impl Stream<Item = (i64, i64, Vec<Event>)> {
        let inner = self.inner.clone();
        async_stream::stream! {
            let cf = match inner.database.cf_handle(CF_IDX) {
                Some(cf) => cf,
                None => return,
            };

            let start_key = encode_key(timeline_id, start_offset);
            let iter = inner.database.iterator_cf(
                &cf,
                rocksdb::IteratorMode::From(&start_key, rocksdb::Direction::Forward),
            );

            let mut current_batch = Vec::new();
            let mut last_offset = start_offset;

            for item in iter {
                match item {
                    Ok((key, value)) => {
                        if key.len() != 16 {
                            continue;
                        }
                        let (key_timeline_id, key_offset) = decode_key(&key);

                        if key_timeline_id != timeline_id {
                            break;
                        }
                        if key_offset > end_offset {
                            break;
                        }

                        match Event::decode(&*value) {
                            Ok(event) => {
                                last_offset = event.offset;
                                current_batch.push(event);
                                if current_batch.len() >= 100 {
                                    yield (last_offset, last_offset, std::mem::take(&mut current_batch));
                                }
                            }
                            Err(_) => {}
                        }
                    }
                    Err(_) => break,
                }
            }

            if !current_batch.is_empty() {
                yield (last_offset, last_offset, current_batch);
            }
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
        // Keys for same timeline should be ordered by offset
        let k1 = encode_key(1, 0);
        let k2 = encode_key(1, 1);
        let k3 = encode_key(1, 100);
        assert!(k1 < k2);
        assert!(k2 < k3);

        // Keys for different timelines should be ordered by timeline_id
        let ka = encode_key(1, 999);
        let kb = encode_key(2, 0);
        assert!(ka < kb);
    }

    #[test]
    fn test_key_negative_values() {
        // Negative values should still round-trip correctly
        let key = encode_key(-1, -100);
        let (tid, off) = decode_key(&key);
        assert_eq!(tid, -1);
        assert_eq!(off, -100);
    }
}
