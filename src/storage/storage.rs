use rocksdb::DB;
use std::sync::Arc;

struct Inner {
    database: DB,
}

pub struct Storage {
    inner: Arc<Inner>,
}

impl Storage {}
