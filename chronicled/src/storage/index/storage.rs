use std::fs;
use std::sync::Arc;

use rocksdb::{DB, DBCompressionType, LogLevel, Options, WriteBatch};

use crate::error::unit_error::UnitError;

use super::entry::{IndexEntry, decode_key, encode_key};

pub(crate) struct Inner {
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
        fs::create_dir_all(&options.path)
            .map_err(|e| UnitError::Storage(format!("failed to create storage directory: {}", e)))?;

        let mut db_options = Options::default();
        db_options.create_if_missing(true);
        db_options.set_compression_type(DBCompressionType::Lz4);
        db_options.set_bottommost_compression_type(DBCompressionType::Zstd);
        db_options.set_log_level(LogLevel::Info);
        db_options.set_keep_log_file_num(10);

        let db = DB::open(&db_options, &options.path)
            .map_err(|err| UnitError::Storage(err.to_string()))?;

        Ok(Storage {
            inner: Arc::new(Inner { database: db }),
        })
    }

    pub fn put_index_batch(
        &self,
        entries: &[((i64, i64), IndexEntry)],
    ) -> Result<(), UnitError> {
        let mut batch = WriteBatch::default();
        for &((timeline_id, offset), ref entry) in entries {
            let key = encode_key(timeline_id, offset);
            let value = entry.encode();
            batch.put(key, value);
        }
        self.inner
            .database
            .write(batch)
            .map_err(|e| UnitError::Storage(e.to_string()))
    }

    pub fn delete_index_range(
        &self,
        timeline_id: i64,
        from_offset: i64,
    ) -> Result<(), UnitError> {
        let db = &self.inner.database;
        let cf = db
            .cf_handle(rocksdb::DEFAULT_COLUMN_FAMILY_NAME)
            .ok_or_else(|| UnitError::Storage("default column family not found".into()))?;
        let start_key = encode_key(timeline_id, from_offset);
        let end_key = encode_key(timeline_id, i64::MAX);
        db.delete_range_cf(&cf, &start_key, &end_key)
            .map_err(|e| UnitError::Storage(e.to_string()))
    }

    pub fn scan_index(
        &self,
        timeline_id: i64,
        start_offset: i64,
        end_offset: i64,
    ) -> Vec<(i64, IndexEntry)> {
        let start_key = encode_key(timeline_id, start_offset);
        let iter = self.inner.database.iterator(
            rocksdb::IteratorMode::From(&start_key, rocksdb::Direction::Forward),
        );

        let mut results = Vec::new();
        for item in iter {
            match item {
                Ok((key, value)) => {
                    if key.len() != 16 || value.len() != 20 {
                        continue;
                    }
                    let (key_timeline_id, key_offset) = decode_key(&key);
                    if key_timeline_id != timeline_id {
                        break;
                    }
                    if key_offset > end_offset {
                        break;
                    }
                    results.push((key_offset, IndexEntry::decode(&value)));
                }
                Err(_) => break,
            }
        }
        results
    }
}
