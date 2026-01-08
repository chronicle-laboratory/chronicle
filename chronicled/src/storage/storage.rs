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
        // LSM
        db_options.set_compression_type(DBCompressionType::Lz4);
        db_options.set_bottommost_compression_type(DBCompressionType::Zstd);
        // Observability
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
                None => {
                    // Log error, column family not found
                    return;
                }
            };

            let start_key = format!("{}-{}", timeline_id, start_offset);
            let mut iter = inner.database.iterator_cf(&cf, rocksdb::IteratorMode::From(start_key.as_bytes(), rocksdb::Direction::Forward));

            let mut current_batch = Vec::new();
            let mut last_offset = start_offset;

            for item in iter {
                match item {
                    Ok((key, value)) => {
                        let key_str = String::from_utf8_lossy(&key);
                        let parts: Vec<&str> = key_str.split('-').collect();
                        if parts.len() != 2 {
                            continue;
                        }
                        let key_timeline_id: i64 = match parts[0].parse() {
                            Ok(id) => id,
                            Err(_) => continue,
                        };
                        let key_offset: i64 = match parts[1].parse() {
                            Ok(offset) => offset,
                            Err(_) => continue,
                        };

                        if key_timeline_id != timeline_id || key_offset > end_offset {
                            break;
                        }

                        match Event::decode(&*value) {
                            Ok(event) => {
                                last_offset = event.offset;
                                current_batch.push(event);
                                if current_batch.len() >= 100 { // Batch size
                                    yield (last_offset, last_offset, std::mem::take(&mut current_batch));
                                }
                            }
                            Err(e) => {
                                // Log decoding error
                                eprintln!("Failed to decode event: {:?}", e);
                            }
                        }
                    }
                    Err(e) => {
                        // Log db error
                        eprintln!("RocksDB iterator error: {:?}", e);
                        break;
                    }
                }
            }

            if !current_batch.is_empty() {
                yield (last_offset, last_offset, current_batch);
            }
        }
    }
}

