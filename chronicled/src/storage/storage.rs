use crate::error::unit_error::UnitError;
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
}
