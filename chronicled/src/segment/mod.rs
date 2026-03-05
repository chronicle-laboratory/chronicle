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
    fn offset(&self) -> u64;
    fn size(&self) -> u64;
}
