//! Lexicon 核心业务逻辑

use std::sync::Arc;

use prost_for_types::Message as _;

use crate::basic_to_json;
use crate::compatibility;
use crate::error::{LexiconError, Result};
use crate::fingerprint;
use crate::store::SchemaStore;
use crate::types::*;

pub struct LexiconService {
    store: Arc<dyn SchemaStore>,
}

impl LexiconService {
    pub fn new(store: Arc<dyn SchemaStore>) -> Self {
        Self { store }
    }

    /// 注册 subject + 初始 schema (version 1)
    pub async fn register_subject(
        &self,
        subject: String,
        wire_format: WireFormat,
        input: HttpSchemaInput,
        compatibility_mode: CompatibilityMode,
    ) -> Result<(SchemaRecord, SubjectMeta)> {
        if self.store.subject_exists(&subject).await? {
            return Err(LexiconError::SubjectAlreadyExists(subject));
        }

        let stored = self.resolve_schema_input(&wire_format, &input)?;
        let fp = fingerprint::compute_fingerprint(&stored)?;
        let now = chrono::Utc::now();

        let record = SchemaRecord {
            subject: subject.clone(),
            version: 1,
            wire_format: wire_format.clone(),
            schema: stored,
            fingerprint: fp,
            created_at: now,
        };

        let meta = SubjectMeta {
            subject: subject.clone(),
            wire_format,
            latest_version: 1,
            total_versions: 1,
            compatibility: compatibility_mode,
            created_at: now,
            updated_at: now,
        };

        self.store.put_schema(&record).await?;
        self.store.put_latest(&record).await?;
        self.store.put_meta(&meta).await?;

        tracing::info!(subject = %subject, "subject registered with schema v1");
        Ok((record, meta))
    }

    /// 注册新 schema 版本
    pub async fn register_schema(
        &self,
        subject: &str,
        input: HttpSchemaInput,
    ) -> Result<(SchemaRecord, bool)> {
        let mut meta = self
            .store
            .get_meta(subject)
            .await?
            .ok_or_else(|| LexiconError::SubjectNotFound(subject.to_string()))?;

        let stored = self.resolve_schema_input(&meta.wire_format, &input)?;
        let fp = fingerprint::compute_fingerprint(&stored)?;

        let latest = self
            .store
            .get_latest(subject)
            .await?
            .ok_or_else(|| LexiconError::SubjectNotFound(subject.to_string()))?;

        if fp == latest.fingerprint {
            tracing::debug!(subject = %subject, "schema unchanged, skipping");
            return Ok((latest, false));
        }

        if meta.compatibility != CompatibilityMode::None {
            let (compatible, violations) =
                compatibility::check_compatibility(&meta.wire_format, &latest.schema, &stored)?;
            if !compatible {
                return Err(LexiconError::IncompatibleSchema { violations });
            }
        }

        let new_version = meta.latest_version + 1;
        let now = chrono::Utc::now();

        let record = SchemaRecord {
            subject: subject.to_string(),
            version: new_version,
            wire_format: meta.wire_format.clone(),
            schema: stored,
            fingerprint: fp,
            created_at: now,
        };

        self.store.put_schema(&record).await?;
        self.store.put_latest(&record).await?;

        meta.latest_version = new_version;
        meta.total_versions = new_version;
        meta.updated_at = now;
        self.store.put_meta(&meta).await?;

        tracing::info!(subject = %subject, version = new_version, "schema registered");
        Ok((record, true))
    }

    pub async fn get_latest(&self, subject: &str) -> Result<SchemaRecord> {
        self.store
            .get_latest(subject)
            .await?
            .ok_or_else(|| LexiconError::SubjectNotFound(subject.to_string()))
    }

    pub async fn get_version(&self, subject: &str, version: u32) -> Result<SchemaRecord> {
        self.store
            .get_version(subject, version)
            .await?
            .ok_or_else(|| LexiconError::VersionNotFound {
                subject: subject.to_string(),
                version,
            })
    }

    pub async fn get_subject(&self, subject: &str) -> Result<SubjectMeta> {
        self.store
            .get_meta(subject)
            .await?
            .ok_or_else(|| LexiconError::SubjectNotFound(subject.to_string()))
    }

    pub async fn list_subjects(&self) -> Result<Vec<SubjectMeta>> {
        self.store.list_subjects().await
    }

    /// 兼容性检查（dry run，不写入）
    pub async fn check_compatibility(
        &self,
        subject: &str,
        input: HttpSchemaInput,
    ) -> Result<(bool, Vec<String>)> {
        let meta = self
            .store
            .get_meta(subject)
            .await?
            .ok_or_else(|| LexiconError::SubjectNotFound(subject.to_string()))?;

        let stored = self.resolve_schema_input(&meta.wire_format, &input)?;

        let latest = self
            .store
            .get_latest(subject)
            .await?
            .ok_or_else(|| LexiconError::SubjectNotFound(subject.to_string()))?;

        compatibility::check_compatibility(&meta.wire_format, &latest.schema, &stored)
    }

    /// 解析 SchemaInput → StoredSchema，并校验与 wire_format 的匹配
    fn resolve_schema_input(
        &self,
        wire_format: &WireFormat,
        input: &HttpSchemaInput,
    ) -> Result<StoredSchema> {
        match (wire_format, input) {
            (WireFormat::Json, HttpSchemaInput::Basic { fields }) => {
                let json_schema = basic_to_json::basic_fields_to_json_schema(fields)?;
                let _: serde_json::Value = serde_json::from_str(&json_schema)?;
                Ok(StoredSchema::JsonSchema {
                    content: json_schema,
                })
            }
            (WireFormat::Json, HttpSchemaInput::JsonSchema { content }) => {
                let _: serde_json::Value = serde_json::from_str(content)
                    .map_err(|e| LexiconError::InvalidSchema(format!("invalid JSON Schema: {e}")))?;
                Ok(StoredSchema::JsonSchema {
                    content: content.clone(),
                })
            }
            (WireFormat::Avro, HttpSchemaInput::Avro { content }) => {
                apache_avro::Schema::parse_str(content)
                    .map_err(|e| LexiconError::InvalidSchema(format!("invalid Avro schema: {e}")))?;
                Ok(StoredSchema::AvroSchema {
                    content: content.clone(),
                })
            }
            (WireFormat::Protobuf, HttpSchemaInput::Protobuf { descriptor, message_name }) => {
                use base64::Engine;
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(descriptor)
                    .map_err(|e| LexiconError::InvalidSchema(format!("invalid base64: {e}")))?;
                let fd = prost_types::FileDescriptorProto::decode(bytes.as_slice())
                    .map_err(|e| LexiconError::InvalidSchema(format!("invalid descriptor: {e}")))?;
                if !fd.message_type.iter().any(|m| m.name.as_deref() == Some(message_name)) {
                    return Err(LexiconError::InvalidSchema(format!(
                        "message {message_name} not found in descriptor"
                    )));
                }
                Ok(StoredSchema::ProtobufDescriptor {
                    descriptor_base64: descriptor.clone(),
                    message_name: message_name.clone(),
                })
            }
            (wire, input) => {
                let input_format = match input {
                    HttpSchemaInput::Basic { .. } => "basic",
                    HttpSchemaInput::JsonSchema { .. } => "json_schema",
                    HttpSchemaInput::Avro { .. } => "avro",
                    HttpSchemaInput::Protobuf { .. } => "protobuf",
                };
                Err(LexiconError::SchemaInputMismatch {
                    wire_format: format!("{wire:?}"),
                    detail: format!(
                        "wire_format={wire} does not accept schema format '{input_format}'"
                    ),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::LocalFileStore;

    async fn test_service() -> (LexiconService, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalFileStore::new(dir.path()).unwrap());
        (LexiconService::new(store), dir)
    }

    fn basic_field(name: &str, field_type: BasicType, nullable: bool) -> BasicField {
        BasicField {
            name: name.to_string(),
            field_type,
            nullable,
            element: None,
            key: None,
            value: None,
            fields: None,
            variants: None,
            precision: None,
            scale: None,
        }
    }

    #[tokio::test]
    async fn test_register_subject() {
        let (svc, _dir) = test_service().await;

        let input = HttpSchemaInput::Basic {
            fields: vec![
                basic_field("id", BasicType::Int64, false),
                basic_field("name", BasicType::String, true),
            ],
        };

        let (record, meta) = svc
            .register_subject("orders-value".into(), WireFormat::Json, input, Default::default())
            .await
            .unwrap();

        assert_eq!(record.version, 1);
        assert_eq!(meta.subject, "orders-value");
        assert_eq!(meta.latest_version, 1);
    }

    #[tokio::test]
    async fn test_register_subject_duplicate() {
        let (svc, _dir) = test_service().await;

        let input = HttpSchemaInput::Basic {
            fields: vec![basic_field("id", BasicType::Int64, false)],
        };

        svc.register_subject("orders-value".into(), WireFormat::Json, input.clone(), Default::default())
            .await
            .unwrap();

        let result = svc
            .register_subject("orders-value".into(), WireFormat::Json, input, Default::default())
            .await;

        assert!(matches!(result, Err(LexiconError::SubjectAlreadyExists(_))));
    }

    #[tokio::test]
    async fn test_register_schema_compatible() {
        let (svc, _dir) = test_service().await;

        let input = HttpSchemaInput::Basic {
            fields: vec![basic_field("id", BasicType::Int64, false)],
        };

        svc.register_subject("orders-value".into(), WireFormat::Json, input, Default::default())
            .await
            .unwrap();

        // 加 nullable 字段 → OK
        let new_input = HttpSchemaInput::Basic {
            fields: vec![
                basic_field("id", BasicType::Int64, false),
                basic_field("name", BasicType::String, true),
            ],
        };

        let (record, is_new) = svc.register_schema("orders-value", new_input).await.unwrap();
        assert!(is_new);
        assert_eq!(record.version, 2);
    }

    #[tokio::test]
    async fn test_register_schema_incompatible() {
        let (svc, _dir) = test_service().await;

        let input = HttpSchemaInput::Basic {
            fields: vec![
                basic_field("id", BasicType::Int64, false),
                basic_field("name", BasicType::String, true),
            ],
        };

        svc.register_subject("orders-value".into(), WireFormat::Json, input, Default::default())
            .await
            .unwrap();

        // 删除字段 → FAIL
        let bad_input = HttpSchemaInput::Basic {
            fields: vec![basic_field("id", BasicType::Int64, false)],
        };

        let result = svc.register_schema("orders-value", bad_input).await;
        assert!(matches!(result, Err(LexiconError::IncompatibleSchema { .. })));
    }

    #[tokio::test]
    async fn test_idempotent_register() {
        let (svc, _dir) = test_service().await;

        let input = HttpSchemaInput::Basic {
            fields: vec![basic_field("id", BasicType::Int64, false)],
        };

        svc.register_subject("orders-value".into(), WireFormat::Json, input.clone(), Default::default())
            .await
            .unwrap();

        let (record, is_new) = svc.register_schema("orders-value", input).await.unwrap();
        assert!(!is_new);
        assert_eq!(record.version, 1);
    }

    #[tokio::test]
    async fn test_wire_format_mismatch() {
        let (svc, _dir) = test_service().await;

        let input = HttpSchemaInput::Basic {
            fields: vec![basic_field("id", BasicType::Int64, false)],
        };

        svc.register_subject("orders-value".into(), WireFormat::Json, input, Default::default())
            .await
            .unwrap();

        let avro_input = HttpSchemaInput::Avro {
            content: r#"{"type":"record","name":"Test","fields":[{"name":"id","type":"long"}]}"#
                .to_string(),
        };

        let result = svc.register_schema("orders-value", avro_input).await;
        assert!(matches!(result, Err(LexiconError::SchemaInputMismatch { .. })));
    }
}
