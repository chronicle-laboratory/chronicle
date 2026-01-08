use crate::error::unit_error::UnitError;
use crate::storage::storage::Inner;
use chronicle_proto::pb_ext::Event;
use prost::Message;
use std::sync::Arc;

pub struct WriteCache {
    storage_inner: Arc<Inner>,
}

impl WriteCache {
    pub fn new(storage_inner: Arc<Inner>) -> Self {
        Self { storage_inner }
    }

    pub fn put_with_trunc(&self, event: Event, _truncate: bool) -> Result<(), UnitError> {
        let db = &self.storage_inner.database;
        let cf = db
            .cf_handle(crate::storage::storage::CF_IDX)
            .ok_or_else(|| UnitError::Storage("Column family 'idx' not found".to_string()))?;

        let key = format!("{}-{}", event.timeline_id, event.offset);
        let value = event.encode_to_vec();

        db.put_cf(cf, key.as_bytes(), value)
            .map_err(|e| UnitError::Storage(e.to_string()))
    }
}
