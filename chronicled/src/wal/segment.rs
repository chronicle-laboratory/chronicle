use std::io::Error;
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

pub struct Segment {
    pub path: PathBuf,
    file: File,
}

impl Segment {
    pub async fn new(path: PathBuf) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        Ok(Segment { path, file })
    }

    pub async fn write(&mut self, data: &[u8]) -> Result<u64, Error> {
        self.file.write_all(data).await?;
        Ok(self.file.metadata().await?.len())
    }

    pub async fn sync(&self) -> Result<(), Error> {
        self.file.sync_data().await
    }
}
