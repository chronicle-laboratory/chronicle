//! Schema 存储层
//!
//! 提供 SchemaStore trait 和两种实现：
//! - LocalFileStore: 本地文件存储（开发/测试）
//! - OxiaStore: Oxia 存储（生产）

use std::path::PathBuf;

use async_trait::async_trait;

use crate::error::{LexiconError, Result};
use crate::types::{SchemaRecord, SubjectMeta};

#[async_trait]
pub trait SchemaStore: Send + Sync {
    async fn put_schema(&self, record: &SchemaRecord) -> Result<()>;
    async fn put_latest(&self, record: &SchemaRecord) -> Result<()>;
    async fn get_latest(&self, subject: &str) -> Result<Option<SchemaRecord>>;
    async fn get_version(&self, subject: &str, version: u32) -> Result<Option<SchemaRecord>>;
    async fn put_meta(&self, meta: &SubjectMeta) -> Result<()>;
    async fn get_meta(&self, subject: &str) -> Result<Option<SubjectMeta>>;
    async fn list_subjects(&self) -> Result<Vec<SubjectMeta>>;
    async fn subject_exists(&self, subject: &str) -> Result<bool>;
}

// ============ LocalFileStore ============

/// 本地文件存储（开发/测试）
///
/// 文件布局:
///   {base_dir}/{subject}/meta.json
///   {base_dir}/{subject}/latest.json
///   {base_dir}/{subject}/versions/{version}.json
pub struct LocalFileStore {
    base_dir: PathBuf,
}

impl LocalFileStore {
    pub fn new(base_dir: impl Into<PathBuf>) -> Result<Self> {
        let base_dir = base_dir.into();
        std::fs::create_dir_all(&base_dir)?;
        Ok(Self { base_dir })
    }

    fn subject_dir(&self, subject: &str) -> PathBuf {
        self.base_dir.join(subject)
    }

    fn meta_path(&self, subject: &str) -> PathBuf {
        self.subject_dir(subject).join("meta.json")
    }

    fn latest_path(&self, subject: &str) -> PathBuf {
        self.subject_dir(subject).join("latest.json")
    }

    fn version_path(&self, subject: &str, version: u32) -> PathBuf {
        self.subject_dir(subject)
            .join("versions")
            .join(format!("{version}.json"))
    }
}

#[async_trait]
impl SchemaStore for LocalFileStore {
    async fn put_schema(&self, record: &SchemaRecord) -> Result<()> {
        let path = self.version_path(&record.subject, record.version);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_string_pretty(record)?;
        std::fs::write(&path, json)?;
        tracing::debug!(subject = %record.subject, version = record.version, "stored schema version");
        Ok(())
    }

    async fn put_latest(&self, record: &SchemaRecord) -> Result<()> {
        let path = self.latest_path(&record.subject);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_string_pretty(record)?;
        std::fs::write(&path, json)?;
        Ok(())
    }

    async fn get_latest(&self, subject: &str) -> Result<Option<SchemaRecord>> {
        let path = self.latest_path(subject);
        if !path.exists() {
            return Ok(None);
        }
        let content = std::fs::read_to_string(&path)?;
        let record: SchemaRecord = serde_json::from_str(&content)?;
        Ok(Some(record))
    }

    async fn get_version(&self, subject: &str, version: u32) -> Result<Option<SchemaRecord>> {
        let path = self.version_path(subject, version);
        if !path.exists() {
            return Ok(None);
        }
        let content = std::fs::read_to_string(&path)?;
        let record: SchemaRecord = serde_json::from_str(&content)?;
        Ok(Some(record))
    }

    async fn put_meta(&self, meta: &SubjectMeta) -> Result<()> {
        let path = self.meta_path(&meta.subject);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_string_pretty(meta)?;
        std::fs::write(&path, json)?;
        Ok(())
    }

    async fn get_meta(&self, subject: &str) -> Result<Option<SubjectMeta>> {
        let path = self.meta_path(subject);
        if !path.exists() {
            return Ok(None);
        }
        let content = std::fs::read_to_string(&path)?;
        let meta: SubjectMeta = serde_json::from_str(&content)?;
        Ok(Some(meta))
    }

    async fn list_subjects(&self) -> Result<Vec<SubjectMeta>> {
        let mut subjects = Vec::new();

        if !self.base_dir.exists() {
            return Ok(subjects);
        }

        for entry in std::fs::read_dir(&self.base_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                let meta_path = entry.path().join("meta.json");
                if meta_path.exists() {
                    let content = std::fs::read_to_string(&meta_path)?;
                    let meta: SubjectMeta = serde_json::from_str(&content)?;
                    subjects.push(meta);
                }
            }
        }

        Ok(subjects)
    }

    async fn subject_exists(&self, subject: &str) -> Result<bool> {
        Ok(self.meta_path(subject).exists())
    }
}

// ============ OxiaStore ============

/// Oxia 存储（生产）
///
/// Key 布局:
///   {prefix}/{subject}/meta
///   {prefix}/{subject}/latest
///   {prefix}/{subject}/versions/{version}
pub struct OxiaStore {
    _prefix: String,
}

impl OxiaStore {
    pub fn _new(_oxia_endpoint: &str, prefix: String) -> Result<Self> {
        // TODO: implement with liboxia
        Ok(Self { _prefix: prefix })
    }
}

#[async_trait]
impl SchemaStore for OxiaStore {
    async fn put_schema(&self, _record: &SchemaRecord) -> Result<()> {
        Err(LexiconError::Store("OxiaStore not yet implemented".into()))
    }
    async fn put_latest(&self, _record: &SchemaRecord) -> Result<()> {
        Err(LexiconError::Store("OxiaStore not yet implemented".into()))
    }
    async fn get_latest(&self, _subject: &str) -> Result<Option<SchemaRecord>> {
        Err(LexiconError::Store("OxiaStore not yet implemented".into()))
    }
    async fn get_version(&self, _subject: &str, _version: u32) -> Result<Option<SchemaRecord>> {
        Err(LexiconError::Store("OxiaStore not yet implemented".into()))
    }
    async fn put_meta(&self, _meta: &SubjectMeta) -> Result<()> {
        Err(LexiconError::Store("OxiaStore not yet implemented".into()))
    }
    async fn get_meta(&self, _subject: &str) -> Result<Option<SubjectMeta>> {
        Err(LexiconError::Store("OxiaStore not yet implemented".into()))
    }
    async fn list_subjects(&self) -> Result<Vec<SubjectMeta>> {
        Err(LexiconError::Store("OxiaStore not yet implemented".into()))
    }
    async fn subject_exists(&self, _subject: &str) -> Result<bool> {
        Err(LexiconError::Store("OxiaStore not yet implemented".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;

    fn test_schema_record(subject: &str, version: u32) -> SchemaRecord {
        SchemaRecord {
            subject: subject.to_string(),
            version,
            wire_format: WireFormat::Json,
            schema: StoredSchema::JsonSchema {
                content: r#"{"type":"object","properties":{"id":{"type":"integer"}}}"#.to_string(),
            },
            fingerprint: "test-fp".to_string(),
            created_at: chrono::Utc::now(),
        }
    }

    fn test_subject_meta(subject: &str) -> SubjectMeta {
        SubjectMeta {
            subject: subject.to_string(),
            wire_format: WireFormat::Json,
            latest_version: 1,
            total_versions: 1,
            compatibility: CompatibilityMode::Forward,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_local_store_put_get_schema() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileStore::new(dir.path()).unwrap();

        let record = test_schema_record("orders-value", 1);
        store.put_schema(&record).await.unwrap();
        store.put_latest(&record).await.unwrap();

        let latest = store.get_latest("orders-value").await.unwrap().unwrap();
        assert_eq!(latest.subject, "orders-value");
        assert_eq!(latest.version, 1);

        let v1 = store.get_version("orders-value", 1).await.unwrap().unwrap();
        assert_eq!(v1.version, 1);

        let v2 = store.get_version("orders-value", 2).await.unwrap();
        assert!(v2.is_none());
    }

    #[tokio::test]
    async fn test_local_store_put_get_meta() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileStore::new(dir.path()).unwrap();

        let meta = test_subject_meta("orders-value");
        store.put_meta(&meta).await.unwrap();

        let loaded = store.get_meta("orders-value").await.unwrap().unwrap();
        assert_eq!(loaded.subject, "orders-value");
        assert_eq!(loaded.wire_format, WireFormat::Json);
    }

    #[tokio::test]
    async fn test_local_store_subject_exists() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileStore::new(dir.path()).unwrap();

        assert!(!store.subject_exists("orders-value").await.unwrap());

        let meta = test_subject_meta("orders-value");
        store.put_meta(&meta).await.unwrap();

        assert!(store.subject_exists("orders-value").await.unwrap());
    }

    #[tokio::test]
    async fn test_local_store_list_subjects() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileStore::new(dir.path()).unwrap();

        store.put_meta(&test_subject_meta("orders-value")).await.unwrap();
        store.put_meta(&test_subject_meta("events-value")).await.unwrap();

        let subjects = store.list_subjects().await.unwrap();
        assert_eq!(subjects.len(), 2);
    }

    #[tokio::test]
    async fn test_local_store_nonexistent_subject() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFileStore::new(dir.path()).unwrap();

        assert!(store.get_meta("nonexistent").await.unwrap().is_none());
        assert!(store.get_latest("nonexistent").await.unwrap().is_none());
    }
}
