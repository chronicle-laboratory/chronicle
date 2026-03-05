pub mod standard;
pub mod direct;
pub mod mmap;
pub mod record;

pub const DEFAULT_MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

#[async_trait::async_trait]
pub trait Segment: Send {
    async fn write(&mut self, data: &[u8]) -> Result<u64, std::io::Error>;
    async fn sync(&self) -> Result<(), std::io::Error>;
    async fn read_all(&mut self) -> Result<Vec<u8>, std::io::Error>;
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<(), std::io::Error>;
    fn offset(&self) -> u64;
    fn size(&self) -> u64;

    /// Returns the underlying file for zero-copy operations.
    /// Default returns None (not supported).
    fn as_std_file(&self) -> Option<&std::fs::File> {
        None
    }

    /// Advance the write offset after an external write (e.g. copy_file_range).
    fn advance_offset(&mut self, bytes: u64) {
        let _ = bytes;
    }
}
