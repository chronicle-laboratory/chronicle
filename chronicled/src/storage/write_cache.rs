use crate::error::unit_error::UnitError;
use crate::storage::storage::{encode_key, Inner, CF_IDX};
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

    pub fn put_with_trunc(&self, event: Event, truncate: bool) -> Result<(), UnitError> {
        let db = &self.storage_inner.database;
        let cf = db
            .cf_handle(CF_IDX)
            .ok_or_else(|| UnitError::Storage("Column family 'idx' not found".to_string()))?;

        let timeline_id = event.timeline_id;
        let offset = event.offset;

        if truncate {
            // Delete all entries for this timeline at offsets >= this offset
            let start_key = encode_key(timeline_id, offset);
            let end_key = encode_key(timeline_id, i64::MAX);
            db.delete_range_cf(&cf, &start_key, &end_key)
                .map_err(|e| UnitError::Storage(e.to_string()))?;
        }

        let key = encode_key(timeline_id, offset);
        let value = event.encode_to_vec();

        db.put_cf(&cf, key, value)
            .map_err(|e| UnitError::Storage(e.to_string()))
    }
}
