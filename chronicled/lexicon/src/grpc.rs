//! gRPC handler — 实现 lexicon.proto 中定义的 Lexicon service

use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::proto;
use crate::service::LexiconService;
use crate::types::*;

pub struct LexiconGrpc {
    service: Arc<LexiconService>,
}

impl LexiconGrpc {
    pub fn new(service: Arc<LexiconService>) -> Self {
        Self { service }
    }
}

#[tonic::async_trait]
impl proto::lexicon_server::Lexicon for LexiconGrpc {
    async fn register_subject(
        &self,
        request: Request<proto::RegisterSubjectRequest>,
    ) -> Result<Response<proto::RegisterSubjectResponse>, Status> {
        let req = request.into_inner();

        let wire_format = proto_wire_format(req.wire_format());
        let compat = proto_compat_mode(req.compatibility());
        let input = proto_schema_input_to_http(req.schema)?;

        let (record, meta) = self
            .service
            .register_subject(req.subject, wire_format, input, compat)
            .await
            .map_err(Status::from)?;

        Ok(Response::new(proto::RegisterSubjectResponse {
            schema: Some(schema_record_to_proto(&record)),
            meta: Some(subject_meta_to_proto(&meta)),
        }))
    }

    async fn register_schema(
        &self,
        request: Request<proto::RegisterSchemaRequest>,
    ) -> Result<Response<proto::RegisterSchemaResponse>, Status> {
        let req = request.into_inner();
        let input = proto_schema_input_to_http(req.schema)?;

        let (record, is_new) = self
            .service
            .register_schema(&req.subject, input)
            .await
            .map_err(Status::from)?;

        Ok(Response::new(proto::RegisterSchemaResponse {
            schema: Some(schema_record_to_proto(&record)),
            is_new_version: is_new,
        }))
    }

    async fn get_latest_schema(
        &self,
        request: Request<proto::GetSchemaRequest>,
    ) -> Result<Response<proto::SchemaRecordProto>, Status> {
        let req = request.into_inner();
        let record = self
            .service
            .get_latest(&req.subject)
            .await
            .map_err(Status::from)?;
        Ok(Response::new(schema_record_to_proto(&record)))
    }

    async fn get_schema_version(
        &self,
        request: Request<proto::GetSchemaVersionRequest>,
    ) -> Result<Response<proto::SchemaRecordProto>, Status> {
        let req = request.into_inner();
        let record = self
            .service
            .get_version(&req.subject, req.version)
            .await
            .map_err(Status::from)?;
        Ok(Response::new(schema_record_to_proto(&record)))
    }

    async fn get_subject(
        &self,
        request: Request<proto::GetSubjectRequest>,
    ) -> Result<Response<proto::SubjectMetaProto>, Status> {
        let req = request.into_inner();
        let meta = self
            .service
            .get_subject(&req.subject)
            .await
            .map_err(Status::from)?;
        Ok(Response::new(subject_meta_to_proto(&meta)))
    }

    async fn list_subjects(
        &self,
        _request: Request<proto::ListSubjectsRequest>,
    ) -> Result<Response<proto::ListSubjectsResponse>, Status> {
        let subjects = self.service.list_subjects().await.map_err(Status::from)?;
        Ok(Response::new(proto::ListSubjectsResponse {
            subjects: subjects.iter().map(subject_meta_to_proto).collect(),
        }))
    }

    async fn check_compatibility(
        &self,
        request: Request<proto::CheckCompatibilityRequest>,
    ) -> Result<Response<proto::CheckCompatibilityResponse>, Status> {
        let req = request.into_inner();
        let input = proto_schema_input_to_http(req.schema)?;

        let (compatible, violations) = self
            .service
            .check_compatibility(&req.subject, input)
            .await
            .map_err(Status::from)?;

        Ok(Response::new(proto::CheckCompatibilityResponse {
            compatible,
            violations,
        }))
    }
}

// ============ 转换辅助函数 ============

fn proto_wire_format(wf: proto::WireFormat) -> WireFormat {
    match wf {
        proto::WireFormat::WireJson => WireFormat::Json,
        proto::WireFormat::WireAvro => WireFormat::Avro,
        proto::WireFormat::WireProtobuf => WireFormat::Protobuf,
    }
}

fn wire_format_to_proto(wf: &WireFormat) -> i32 {
    match wf {
        WireFormat::Json => proto::WireFormat::WireJson as i32,
        WireFormat::Avro => proto::WireFormat::WireAvro as i32,
        WireFormat::Protobuf => proto::WireFormat::WireProtobuf as i32,
    }
}

fn proto_compat_mode(cm: proto::CompatibilityMode) -> CompatibilityMode {
    match cm {
        proto::CompatibilityMode::CompatForward => CompatibilityMode::Forward,
        proto::CompatibilityMode::CompatNone => CompatibilityMode::None,
    }
}

fn compat_mode_to_proto(cm: &CompatibilityMode) -> i32 {
    match cm {
        CompatibilityMode::Forward => proto::CompatibilityMode::CompatForward as i32,
        CompatibilityMode::None => proto::CompatibilityMode::CompatNone as i32,
    }
}

/// Proto SchemaInput → HttpSchemaInput
fn proto_schema_input_to_http(
    input: Option<proto::SchemaInput>,
) -> Result<HttpSchemaInput, Status> {
    let input = input.ok_or_else(|| Status::invalid_argument("missing schema input"))?;
    let format = input
        .format
        .ok_or_else(|| Status::invalid_argument("missing schema format"))?;

    match format {
        proto::schema_input::Format::Basic(basic) => {
            let fields = basic
                .fields
                .iter()
                .map(proto_field_to_basic)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(HttpSchemaInput::Basic { fields })
        }
        proto::schema_input::Format::JsonSchema(content) => {
            Ok(HttpSchemaInput::JsonSchema { content })
        }
        proto::schema_input::Format::AvroSchema(content) => {
            Ok(HttpSchemaInput::Avro { content })
        }
        proto::schema_input::Format::Protobuf(pb) => Ok(HttpSchemaInput::Protobuf {
            descriptor: base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                &pb.descriptor,
            ),
            message_name: pb.message_name,
        }),
    }
}

fn proto_field_to_basic(field: &proto::BasicFieldProto) -> Result<BasicField, Status> {
    let field_type = parse_basic_type(&field.field_type)?;

    let element = field
        .element
        .as_ref()
        .map(|e| proto_type_ref_to_basic(e))
        .transpose()?
        .map(Box::new);

    let key = field
        .key
        .as_ref()
        .map(|k| proto_type_ref_to_basic(k))
        .transpose()?
        .map(Box::new);

    let value = field
        .value
        .as_ref()
        .map(|v| proto_type_ref_to_basic(v))
        .transpose()?
        .map(Box::new);

    let fields = if field.fields.is_empty() {
        None
    } else {
        Some(
            field
                .fields
                .iter()
                .map(proto_field_to_basic)
                .collect::<Result<Vec<_>, _>>()?,
        )
    };

    let variants = if field.variants.is_empty() {
        None
    } else {
        Some(field.variants.clone())
    };

    Ok(BasicField {
        name: field.name.clone(),
        field_type,
        nullable: field.nullable,
        element,
        key,
        value,
        fields,
        variants,
        precision: field.precision.map(|p| p as u8),
        scale: field.scale.map(|s| s as i8),
    })
}

fn proto_type_ref_to_basic(r: &proto::BasicTypeRef) -> Result<BasicTypeOrRef, Status> {
    let field_type = parse_basic_type(&r.field_type)?;
    Ok(BasicTypeOrRef::Simple { field_type })
}

fn parse_basic_type(s: &str) -> Result<BasicType, Status> {
    match s {
        "bool" => Ok(BasicType::Bool),
        "int32" => Ok(BasicType::Int32),
        "int64" => Ok(BasicType::Int64),
        "float32" => Ok(BasicType::Float32),
        "float64" => Ok(BasicType::Float64),
        "string" => Ok(BasicType::String),
        "binary" => Ok(BasicType::Binary),
        "timestamp_ms" => Ok(BasicType::TimestampMs),
        "timestamp_us" => Ok(BasicType::TimestampUs),
        "date" => Ok(BasicType::Date),
        "time_ms" => Ok(BasicType::TimeMs),
        "decimal" => Ok(BasicType::Decimal),
        "enum" => Ok(BasicType::Enum),
        "list" => Ok(BasicType::List),
        "map" => Ok(BasicType::Map),
        "struct" => Ok(BasicType::Struct),
        _ => Err(Status::invalid_argument(format!(
            "unknown basic type: {s}"
        ))),
    }
}

fn schema_record_to_proto(record: &SchemaRecord) -> proto::SchemaRecordProto {
    proto::SchemaRecordProto {
        subject: record.subject.clone(),
        version: record.version,
        wire_format: wire_format_to_proto(&record.wire_format),
        schema: Some(stored_schema_to_proto(&record.schema)),
        fingerprint: record.fingerprint.clone(),
        created_at_ms: record.created_at.timestamp_millis(),
    }
}

fn stored_schema_to_proto(schema: &StoredSchema) -> proto::StoredSchemaProto {
    match schema {
        StoredSchema::JsonSchema { content } => proto::StoredSchemaProto {
            content: Some(proto::stored_schema_proto::Content::JsonSchema(
                content.clone(),
            )),
        },
        StoredSchema::AvroSchema { content } => proto::StoredSchemaProto {
            content: Some(proto::stored_schema_proto::Content::AvroSchema(
                content.clone(),
            )),
        },
        StoredSchema::ProtobufDescriptor {
            descriptor_base64,
            message_name,
        } => {
            use base64::Engine;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(descriptor_base64)
                .unwrap_or_default();
            proto::StoredSchemaProto {
                content: Some(proto::stored_schema_proto::Content::Protobuf(
                    proto::ProtobufSchemaStored {
                        descriptor: bytes.into(),
                        message_name: message_name.clone(),
                    },
                )),
            }
        }
    }
}

fn subject_meta_to_proto(meta: &SubjectMeta) -> proto::SubjectMetaProto {
    proto::SubjectMetaProto {
        subject: meta.subject.clone(),
        wire_format: wire_format_to_proto(&meta.wire_format),
        latest_version: meta.latest_version,
        total_versions: meta.total_versions,
        compatibility: compat_mode_to_proto(&meta.compatibility),
    }
}
