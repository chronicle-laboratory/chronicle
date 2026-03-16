pub mod compaction;
pub mod compaction_level;
pub mod compaction_l1;
pub mod compaction_l2;
pub mod compaction_l3;
pub mod compaction_l4;
pub mod copy;
pub mod manager;
pub mod remote;
pub mod reader;
pub mod writer;

pub use reader::BlobReader;
pub use writer::BlobWriter;

pub const ENTRY_HEADER_SIZE: usize = 20;
