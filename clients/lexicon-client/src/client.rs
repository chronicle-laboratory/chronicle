//! Lexicon gRPC 客户端，带本地缓存

use std::collections::HashMap;

use arrow::datatypes::SchemaRef;
use tokio::sync::RwLock;
use tonic::transport::Channel;

use crate::arrow_convert::stored_schema_to_arrow;
use crate::proto;
use chronicle_lexicon::{SchemaRecord, StoredSchema, SubjectMeta, WireFormat};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("grpc error: {0}")]
    Grpc(#[from] tonic::Status),
    #[error("schema conversion error: {0}")]
    SchemaConversion(String),
    #[error("missing field in response: {0}")]
    MissingField(String),
}

pub type Result<T> = std::result::Result<T, ClientError>;

struct CachedEntry {
    record: SchemaRecord,
    arrow_schema: SchemaRef,
    _meta: SubjectMeta,
}

pub struct LexiconClient {
    inner: proto::lexicon_client::LexiconClient<Channel>,
    cache: RwLock<HashMap<String, CachedEntry>>,
}

impl LexiconClient {
    pub async fn connect(endpoint: &str) -> Result<Self> {
        let channel = Channel::from_shared(endpoint.to_string())
            .map_err(|e| ClientError::SchemaConversion(e.to_string()))?
            .connect()
            .await?;

        Ok(Self {
            inner: proto::lexicon_client::LexiconClient::new(channel),
            cache: RwLock::new(HashMap::new()),
        })
    }

    // ===== Producer 侧 =====

    pub async fn register_subject(
        &self,
        req: proto::RegisterSubjectRequest,
    ) -> Result<(SchemaRecord, SubjectMeta)> {
        let mut client = self.inner.clone();
        let resp = client.register_subject(req).await?.into_inner();

        let record = proto_to_schema_record(
            resp.schema
                .ok_or_else(|| ClientError::MissingField("schema".into()))?,
        )?;
        let meta = proto_to_subject_meta(
            resp.meta
                .ok_or_else(|| ClientError::MissingField("meta".into()))?,
        );

        let arrow_schema = stored_schema_to_arrow(&record.schema)
            .map_err(|e| ClientError::SchemaConversion(e.to_string()))?;
        let mut cache = self.cache.write().await;
        cache.insert(
            record.subject.clone(),
            CachedEntry {
                record: record.clone(),
                arrow_schema,
                _meta: meta.clone(),
            },
        );

        Ok((record, meta))
    }

    pub async fn register_schema(
        &self,
        subject: &str,
        input: proto::SchemaInput,
    ) -> Result<(SchemaRecord, bool)> {
        let mut client = self.inner.clone();
        let resp = client
            .register_schema(proto::RegisterSchemaRequest {
                subject: subject.to_string(),
                schema: Some(input),
            })
            .await?
            .into_inner();

        let record = proto_to_schema_record(
            resp.schema
                .ok_or_else(|| ClientError::MissingField("schema".into()))?,
        )?;

        if resp.is_new_version {
            let arrow_schema = stored_schema_to_arrow(&record.schema)
                .map_err(|e| ClientError::SchemaConversion(e.to_string()))?;
            let mut cache = self.cache.write().await;
            if let Some(entry) = cache.get_mut(subject) {
                entry.record = record.clone();
                entry.arrow_schema = arrow_schema;
            }
        }

        Ok((record, resp.is_new_version))
    }

    pub async fn check_compatibility(
        &self,
        subject: &str,
        input: proto::SchemaInput,
    ) -> Result<(bool, Vec<String>)> {
        let mut client = self.inner.clone();
        let resp = client
            .check_compatibility(proto::CheckCompatibilityRequest {
                subject: subject.to_string(),
                schema: Some(input),
            })
            .await?
            .into_inner();

        Ok((resp.compatible, resp.violations))
    }

    // ===== Consumer / Saga 侧 =====

    /// 获取 subject 的最新 schema + Arrow SchemaRef（优先从缓存取）
    pub async fn get_schema(&self, subject: &str) -> Result<(SchemaRecord, SchemaRef)> {
        {
            let cache = self.cache.read().await;
            if let Some(entry) = cache.get(subject) {
                return Ok((entry.record.clone(), entry.arrow_schema.clone()));
            }
        }

        let mut client = self.inner.clone();
        let resp = client
            .get_latest_schema(proto::GetSchemaRequest {
                subject: subject.to_string(),
            })
            .await?
            .into_inner();

        let record = proto_to_schema_record(resp)?;
        let arrow_schema = stored_schema_to_arrow(&record.schema)
            .map_err(|e| ClientError::SchemaConversion(e.to_string()))?;

        let meta_resp = {
            let mut c = self.inner.clone();
            c.get_subject(proto::GetSubjectRequest {
                subject: subject.to_string(),
            })
            .await?
            .into_inner()
        };
        let meta = proto_to_subject_meta(meta_resp);

        let mut cache = self.cache.write().await;
        cache.insert(
            subject.to_string(),
            CachedEntry {
                record: record.clone(),
                arrow_schema: arrow_schema.clone(),
                _meta: meta,
            },
        );

        Ok((record, arrow_schema))
    }

    pub async fn get_schema_version(
        &self,
        subject: &str,
        version: u32,
    ) -> Result<(SchemaRecord, SchemaRef)> {
        let mut client = self.inner.clone();
        let resp = client
            .get_schema_version(proto::GetSchemaVersionRequest {
                subject: subject.to_string(),
                version,
            })
            .await?
            .into_inner();

        let record = proto_to_schema_record(resp)?;
        let arrow_schema = stored_schema_to_arrow(&record.schema)
            .map_err(|e| ClientError::SchemaConversion(e.to_string()))?;

        Ok((record, arrow_schema))
    }

    pub async fn get_subject(&self, subject: &str) -> Result<SubjectMeta> {
        let mut client = self.inner.clone();
        let resp = client
            .get_subject(proto::GetSubjectRequest {
                subject: subject.to_string(),
            })
            .await?
            .into_inner();
        Ok(proto_to_subject_meta(resp))
    }

    pub async fn refresh(&self, subject: &str) -> Result<()> {
        self.cache.write().await.remove(subject);
        self.get_schema(subject).await?;
        Ok(())
    }

    pub async fn list_subjects(&self) -> Result<Vec<SubjectMeta>> {
        let mut client = self.inner.clone();
        let resp = client
            .list_subjects(proto::ListSubjectsRequest {})
            .await?
            .into_inner();

        Ok(resp
            .subjects
            .into_iter()
            .map(proto_to_subject_meta)
            .collect())
    }
}

// ============ Proto 转换 ============

fn proto_to_schema_record(p: proto::SchemaRecordProto) -> Result<SchemaRecord> {
    let wire_format = match proto::WireFormat::try_from(p.wire_format) {
        Ok(proto::WireFormat::WireJson) => WireFormat::Json,
        Ok(proto::WireFormat::WireAvro) => WireFormat::Avro,
        Ok(proto::WireFormat::WireProtobuf) => WireFormat::Protobuf,
        Err(_) => WireFormat::Json,
    };

    let schema = proto_to_stored_schema(
        p.schema
            .ok_or_else(|| ClientError::MissingField("stored schema".into()))?,
    );

    Ok(SchemaRecord {
        subject: p.subject,
        version: p.version,
        wire_format,
        schema,
        fingerprint: p.fingerprint,
        created_at: chrono::DateTime::from_timestamp_millis(p.created_at_ms)
            .unwrap_or_else(chrono::Utc::now),
    })
}

fn proto_to_stored_schema(p: proto::StoredSchemaProto) -> StoredSchema {
    match p.content {
        Some(proto::stored_schema_proto::Content::JsonSchema(s)) => {
            StoredSchema::JsonSchema { content: s }
        }
        Some(proto::stored_schema_proto::Content::AvroSchema(s)) => {
            StoredSchema::AvroSchema { content: s }
        }
        Some(proto::stored_schema_proto::Content::Protobuf(pb)) => {
            use base64::Engine;
            StoredSchema::ProtobufDescriptor {
                descriptor_base64: base64::engine::general_purpose::STANDARD
                    .encode(&pb.descriptor),
                message_name: pb.message_name,
            }
        }
        None => StoredSchema::JsonSchema {
            content: "{}".to_string(),
        },
    }
}

fn proto_to_subject_meta(p: proto::SubjectMetaProto) -> SubjectMeta {
    use chronicle_lexicon::CompatibilityMode;

    let wire_format = match proto::WireFormat::try_from(p.wire_format) {
        Ok(proto::WireFormat::WireJson) => WireFormat::Json,
        Ok(proto::WireFormat::WireAvro) => WireFormat::Avro,
        Ok(proto::WireFormat::WireProtobuf) => WireFormat::Protobuf,
        Err(_) => WireFormat::Json,
    };

    let compatibility = match proto::CompatibilityMode::try_from(p.compatibility) {
        Ok(proto::CompatibilityMode::CompatForward) => CompatibilityMode::Forward,
        Ok(proto::CompatibilityMode::CompatNone) => CompatibilityMode::None,
        Err(_) => CompatibilityMode::Forward,
    };

    SubjectMeta {
        subject: p.subject,
        wire_format,
        latest_version: p.latest_version,
        total_versions: p.total_versions,
        compatibility,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    }
}
