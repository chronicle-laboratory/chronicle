use chronicle_proto::pb_ext::Event;
use futures_util::Stream;

pub mod storage;
pub mod write_cache;

pub trait TimelineReader {
    fn fetch_batches(
        &self,
        timeline_id: i64,
        start_offset: i64,
        end_offset: i64,
    ) -> impl Stream<Item = (i64, i64, Vec<Event>)>;
}
