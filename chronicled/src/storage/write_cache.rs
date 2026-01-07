use crate::storage::TimelineReader;
use crate::storage::storage::Storage;
use chronicle_proto::pb_ext::Event;

#[derive(Clone)]
pub struct WriteCache {}

impl WriteCache {
    pub fn put_with_trunc(&self, event: Event, truncate: bool) {
        todo!()
    }
}
