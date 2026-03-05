pub mod compaction;
pub mod manager;
pub mod remote;
pub mod reader;
pub mod writer;

pub use reader::BlobReader;
pub use writer::BlobWriter;

pub const ENTRY_HEADER_SIZE: usize = 20;
