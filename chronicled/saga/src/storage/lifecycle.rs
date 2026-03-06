use crate::error::{Result, SagaError};
use crate::types::Watermarks;
use parking_lot::RwLock;
use std::path::PathBuf;
use tracing::info;

/// Watermark management, checkpoint persistence, and recovery.
pub struct Lifecycle {
    watermarks: RwLock<Watermarks>,
    checkpoint_path: PathBuf,
}

impl Lifecycle {
    pub fn new(checkpoint_path: PathBuf) -> Self {
        Self {
            watermarks: RwLock::new(Watermarks::default()),
            checkpoint_path,
        }
    }

    pub fn advance_ingested(&self, seq: u64) {
        let mut wm = self.watermarks.write();
        if seq > wm.ingested {
            wm.ingested = seq;
        }
    }

    pub fn advance_flushed(&self, seq: u64) {
        let mut wm = self.watermarks.write();
        if seq > wm.flushed {
            wm.flushed = seq;
        }
    }

    pub fn advance_persisted(&self, seq: u64) {
        let mut wm = self.watermarks.write();
        if seq > wm.persisted {
            wm.persisted = seq;
        }
    }

    pub fn watermarks(&self) -> Watermarks {
        self.watermarks.read().clone()
    }

    /// Persist watermarks to a JSON checkpoint file.
    pub async fn checkpoint(&self) -> Result<()> {
        let wm = self.watermarks.read().clone();
        let json = serde_json::to_string_pretty(&wm)
            .map_err(|e| SagaError::Internal(format!("serialize watermarks: {}", e)))?;

        if let Some(parent) = self.checkpoint_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&self.checkpoint_path, json).await?;

        info!(
            ingested = wm.ingested,
            flushed = wm.flushed,
            persisted = wm.persisted,
            "watermark checkpoint saved"
        );
        Ok(())
    }

    /// Recover watermarks from checkpoint file on startup.
    pub async fn recover(path: PathBuf) -> Result<Self> {
        let lifecycle = if path.exists() {
            let contents = tokio::fs::read_to_string(&path).await?;
            let wm: Watermarks = serde_json::from_str(&contents)
                .map_err(|e| SagaError::Config(format!("parse checkpoint: {}", e)))?;
            info!(
                ingested = wm.ingested,
                flushed = wm.flushed,
                persisted = wm.persisted,
                "watermark checkpoint recovered"
            );
            Self {
                watermarks: RwLock::new(wm),
                checkpoint_path: path,
            }
        } else {
            info!("no checkpoint file found, starting fresh");
            Self::new(path)
        };
        Ok(lifecycle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advance_watermarks() {
        let lc = Lifecycle::new(PathBuf::from("/tmp/test_checkpoint.json"));
        lc.advance_ingested(10);
        lc.advance_flushed(5);
        lc.advance_persisted(2);

        let wm = lc.watermarks();
        assert_eq!(wm.ingested, 10);
        assert_eq!(wm.flushed, 5);
        assert_eq!(wm.persisted, 2);
    }

    #[test]
    fn advance_only_increases() {
        let lc = Lifecycle::new(PathBuf::from("/tmp/test.json"));
        lc.advance_ingested(10);
        lc.advance_ingested(5); // should not decrease
        assert_eq!(lc.watermarks().ingested, 10);
    }

    #[tokio::test]
    async fn checkpoint_and_recover() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");

        let lc = Lifecycle::new(path.clone());
        lc.advance_ingested(42);
        lc.advance_flushed(30);
        lc.checkpoint().await.unwrap();

        let recovered = Lifecycle::recover(path).await.unwrap();
        let wm = recovered.watermarks();
        assert_eq!(wm.ingested, 42);
        assert_eq!(wm.flushed, 30);
    }
}
