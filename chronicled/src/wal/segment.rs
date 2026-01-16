use std::io::Error;
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub struct Segment {
    pub path: PathBuf,
    file: File,
    write_offset: u64,
}

impl Segment {
    pub async fn new(path: PathBuf) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .await?;
        
        let write_offset = file.metadata().await?.len();
        
        Ok(Segment { 
            path, 
            file,
            write_offset,
        })
    }

    /// Write data to the segment and return the offset where data was written
    pub async fn write(&mut self, data: &[u8]) -> Result<u64, Error> {
        let offset_before = self.write_offset;
        self.file.write_all(data).await?;
        self.write_offset += data.len() as u64;
        Ok(offset_before)
    }

    /// Sync data to disk
    pub async fn sync(&self) -> Result<(), Error> {
        self.file.sync_data().await
    }

    /// Get current write offset (file size)
    pub fn offset(&self) -> u64 {
        self.write_offset
    }

    /// Get segment size
    pub fn size(&self) -> u64 {
        self.write_offset
    }

    /// Read data from the segment starting at the given offset
    pub async fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize, Error> {
        self.file.seek(std::io::SeekFrom::Start(offset)).await?;
        self.file.read(buf).await
    }

    /// Read all data from the segment starting at offset 0
    pub async fn read_all(&mut self) -> Result<Vec<u8>, Error> {
        self.file.seek(std::io::SeekFrom::Start(0)).await?;
        let mut buf = Vec::new();
        self.file.read_to_end(&mut buf).await?;
        Ok(buf)
    }
}
