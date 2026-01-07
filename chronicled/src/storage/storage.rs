use crate::error::unit_error::UnitError;
use crate::storage::TimelineReader;
use crate::storage::write_cache::WriteCache;
use chronicle_proto::pb_ext::Event;
use futures_util::Stream;
use rocksdb::{ColumnFamilyDescriptor, DB, DBCompressionType, LogLevel, Options};
use std::sync::Arc;

const CF_META: &str = "meta";
const CF_IDX: &str = "idx";

struct Inner {
    database: DB,
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

    pub(crate) fn fetch_write_cache(&self, index: i32) -> WriteCache {
        todo!()
    }
}

impl TimelineReader for Storage {
    fn fetch_batches(
        &self,
        timeline_id: i64,
        start_offset: i64,
        end_offset: i64,
    ) -> impl Stream<Item = Vec<Event>> {
        todo!()
    }
}
