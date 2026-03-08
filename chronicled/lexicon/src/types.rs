use serde::{Deserialize, Serialize};

/// 注册 subject 时指定的 wire format，不可变
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WireFormat {
    Json,
    Avro,
    Protobuf,
}

impl std::fmt::Display for WireFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WireFormat::Json => write!(f, "json"),
            WireFormat::Avro => write!(f, "avro"),
            WireFormat::Protobuf => write!(f, "protobuf"),
        }
    }
}

/// Schema 的存储内容，按 wire_format 决定
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum StoredSchema {
    /// 内部生成的 JSON Schema（来自基础类型或用户直接提交的 JSON Schema）
    JsonSchema { content: String },
    /// 用户原样提交的 Avro Schema JSON
    AvroSchema { content: String },
    /// 用户原样提交的 Protobuf FileDescriptorProto bytes（base64 encoded）
    ProtobufDescriptor {
        descriptor_base64: String,
        message_name: String,
    },
}

/// 一条 schema 记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRecord {
    pub subject: String,
    pub version: u32,
    pub wire_format: WireFormat,
    pub schema: StoredSchema,
    pub fingerprint: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Subject 元信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubjectMeta {
    pub subject: String,
    pub wire_format: WireFormat,
    pub latest_version: u32,
    pub total_versions: u32,
    pub compatibility: CompatibilityMode,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompatibilityMode {
    Forward,
    None,
}

impl Default for CompatibilityMode {
    fn default() -> Self {
        Self::Forward
    }
}

// ============ 基础类型系统 ============

fn default_true() -> bool {
    true
}

/// 基础类型 field 描述（用户面向的简单 API）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BasicField {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: BasicType,
    #[serde(default = "default_true")]
    pub nullable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub element: Option<Box<BasicTypeOrRef>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<Box<BasicTypeOrRef>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<Box<BasicTypeOrRef>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<BasicField>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variants: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub precision: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scale: Option<i8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BasicType {
    Bool,
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Binary,
    TimestampMs,
    TimestampUs,
    Date,
    TimeMs,
    Decimal,
    Enum,
    List,
    Map,
    Struct,
}

/// 类型引用，用于 list/map 的嵌套
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BasicTypeOrRef {
    Simple {
        #[serde(rename = "type")]
        field_type: BasicType,
    },
    Full(BasicField),
}

// ============ HTTP API 请求体 ============

/// HTTP 注册 subject 请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpRegisterSubjectRequest {
    pub subject: String,
    #[serde(default = "default_json_wire_format")]
    pub wire_format: WireFormat,
    pub schema: HttpSchemaInput,
    #[serde(default)]
    pub compatibility: CompatibilityMode,
}

fn default_json_wire_format() -> WireFormat {
    WireFormat::Json
}

/// HTTP 注册新 schema 版本请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpRegisterSchemaRequest {
    pub schema: HttpSchemaInput,
}

/// HTTP schema 输入，支持四种用法
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "format")]
pub enum HttpSchemaInput {
    /// 基础类型 fields 数组
    #[serde(rename = "basic")]
    Basic { fields: Vec<BasicField> },
    /// 直接提交 JSON Schema
    #[serde(rename = "json_schema")]
    JsonSchema { content: String },
    /// Avro Schema JSON
    #[serde(rename = "avro")]
    Avro { content: String },
    /// Protobuf descriptor
    #[serde(rename = "protobuf")]
    Protobuf {
        descriptor: String,
        message_name: String,
    },
}

/// HTTP 兼容性检查请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpCheckCompatibilityRequest {
    pub schema: HttpSchemaInput,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wire_format_serde() {
        let wf = WireFormat::Json;
        let json = serde_json::to_string(&wf).unwrap();
        assert_eq!(json, r#""json""#);
        let parsed: WireFormat = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, WireFormat::Json);
    }

    #[test]
    fn test_basic_field_serde() {
        let json = r#"{
            "name": "id",
            "type": "int64",
            "nullable": false
        }"#;
        let field: BasicField = serde_json::from_str(json).unwrap();
        assert_eq!(field.name, "id");
        assert_eq!(field.field_type, BasicType::Int64);
        assert!(!field.nullable);
    }

    #[test]
    fn test_http_schema_input_basic() {
        let json = r#"{
            "format": "basic",
            "fields": [
                {"name": "id", "type": "int64", "nullable": false}
            ]
        }"#;
        let input: HttpSchemaInput = serde_json::from_str(json).unwrap();
        match input {
            HttpSchemaInput::Basic { fields } => {
                assert_eq!(fields.len(), 1);
                assert_eq!(fields[0].name, "id");
            }
            _ => panic!("expected Basic"),
        }
    }

    #[test]
    fn test_http_register_subject_request() {
        let json = r#"{
            "subject": "orders-value",
            "schema": {
                "format": "basic",
                "fields": [
                    {"name": "id", "type": "int64", "nullable": false},
                    {"name": "name", "type": "string"}
                ]
            }
        }"#;
        let req: HttpRegisterSubjectRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.subject, "orders-value");
        assert_eq!(req.wire_format, WireFormat::Json); // default
    }

    #[test]
    fn test_stored_schema_serde() {
        let schema = StoredSchema::JsonSchema {
            content: r#"{"type":"object"}"#.to_string(),
        };
        let json = serde_json::to_string(&schema).unwrap();
        let parsed: StoredSchema = serde_json::from_str(&json).unwrap();
        match parsed {
            StoredSchema::JsonSchema { content } => {
                assert!(content.contains("object"));
            }
            _ => panic!("expected JsonSchema"),
        }
    }
}
