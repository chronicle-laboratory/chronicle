use chronicle_proto::pb_ext::Event;

pub mod blob;
pub mod index;
pub mod level_iterator;
pub mod write_cache;

pub trait TimelineReader {
    fn fetch_batches(
        &self,
        timeline_id: i64,
        start_offset: i64,
        end_offset: i64,
    ) -> impl Iterator<Item = (i64, i64, Vec<Event>)>;
}
