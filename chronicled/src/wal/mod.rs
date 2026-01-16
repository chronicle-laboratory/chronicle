mod segment;
pub mod wal;
mod record;

pub use record::{Record, RecordBatch};

const INVALID_OFFSET: i64 = -1;
