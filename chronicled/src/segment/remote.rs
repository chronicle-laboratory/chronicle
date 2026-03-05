use super::Segment;
use std::io::Error;

/// A read-only segment backed by data downloaded from remote storage.
/// The entire segment content is held in memory.
pub struct RemoteSegment {
    data: Vec<u8>,
}

impl RemoteSegment {
    pub fn from_bytes(data: Vec<u8>) -> Self {
        Self { data }
    }
}

#[async_trait::async_trait]
impl Segment for RemoteSegment {
    async fn write(&mut self, _data: &[u8]) -> Result<u64, Error> {
        Err(Error::new(
            std::io::ErrorKind::Unsupported,
            "remote segment is read-only",
        ))
    }

    async fn sync(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn read_all(&mut self) -> Result<Vec<u8>, Error> {
        Ok(self.data.clone())
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<(), Error> {
        let start = offset as usize;
        let end = start + buf.len();
        if end > self.data.len() {
            return Err(Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "read past end of remote segment",
            ));
        }
        buf.copy_from_slice(&self.data[start..end]);
        Ok(())
    }

    fn offset(&self) -> u64 {
        self.data.len() as u64
    }

    fn size(&self) -> u64 {
        self.data.len() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_remote_segment_read_at() {
        let data = b"hello world, this is remote segment data".to_vec();
        let seg = RemoteSegment::from_bytes(data.clone());

        let mut buf = vec![0u8; 5];
        seg.read_at(&mut buf, 0).unwrap();
        assert_eq!(&buf, b"hello");

        seg.read_at(&mut buf, 6).unwrap();
        assert_eq!(&buf, b"world");
    }

    #[tokio::test]
    async fn test_remote_segment_read_all() {
        let data = b"full content".to_vec();
        let mut seg = RemoteSegment::from_bytes(data.clone());
        assert_eq!(seg.read_all().await.unwrap(), data);
    }

    #[tokio::test]
    async fn test_remote_segment_write_fails() {
        let mut seg = RemoteSegment::from_bytes(vec![]);
        assert!(seg.write(b"data").await.is_err());
    }

    #[tokio::test]
    async fn test_remote_segment_read_out_of_bounds() {
        let seg = RemoteSegment::from_bytes(b"short".to_vec());
        let mut buf = vec![0u8; 10];
        assert!(seg.read_at(&mut buf, 0).is_err());
    }
}
