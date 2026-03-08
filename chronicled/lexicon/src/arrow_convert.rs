//! Schema → Arrow DataType 转换
//!
//! 三种格式各自独立转换为 Arrow Schema，不经过中间层。

use std::sync::Arc;

use arrow::datatypes::{
    DataType, Field, Schema, SchemaRef, TimeUnit,
};
use serde_json::Value;

use prost_for_types::Message as _;

use crate::error::{LexiconError, Result};
use crate::types::StoredSchema;

/// 统一入口: StoredSchema → Arrow SchemaRef
pub fn stored_schema_to_arrow(schema: &StoredSchema) -> Result<SchemaRef> {
    match schema {
        StoredSchema::JsonSchema { content } => json_schema_to_arrow(content),
        StoredSchema::AvroSchema { content } => avro_schema_to_arrow(content),
        StoredSchema::ProtobufDescriptor {
            descriptor_base64,
            message_name,
        } => {
            use base64::Engine;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(descriptor_base64)
                .map_err(|e| LexiconError::InvalidSchema(format!("invalid base64: {e}")))?;
            protobuf_to_arrow(&bytes, message_name)
        }
    }
}

/// JSON Schema string → Arrow SchemaRef
///
/// 解析 JSON Schema 的 properties，按 type+format 映射到 Arrow DataType。
/// 映射表与 basic_to_json.rs 完全一致。
pub fn json_schema_to_arrow(json_schema: &str) -> Result<SchemaRef> {
    let value: Value = serde_json::from_str(json_schema)?;
    let obj = value
        .as_object()
        .ok_or_else(|| LexiconError::InvalidSchema("root must be object".into()))?;

    let properties = obj
        .get("properties")
        .and_then(|v| v.as_object())
        .ok_or_else(|| LexiconError::InvalidSchema("missing properties".into()))?;

    let required: Vec<String> = obj
        .get("required")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default();

    let mut fields = Vec::new();
    for (name, prop) in properties {
        let nullable = !required.contains(name);
        let data_type = json_prop_to_arrow_type(prop)?;
        fields.push(Field::new(name, data_type, nullable));
    }

    Ok(Arc::new(Schema::new(fields)))
}

/// 单个 JSON Schema property → Arrow DataType
fn json_prop_to_arrow_type(prop: &Value) -> Result<DataType> {
    let obj = prop
        .as_object()
        .ok_or_else(|| LexiconError::InvalidSchema("property must be object".into()))?;

    // 检查 enum
    if obj.contains_key("enum") {
        return Ok(DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8),
        ));
    }

    let type_str = obj
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| LexiconError::InvalidSchema("property missing type".into()))?;

    let format_str = obj.get("format").and_then(|v| v.as_str());

    match (type_str, format_str) {
        ("boolean", _) => Ok(DataType::Boolean),
        ("integer", Some("int32")) => Ok(DataType::Int32),
        ("integer", _) => Ok(DataType::Int64),
        ("number", Some("float")) => Ok(DataType::Float32),
        ("number", _) => Ok(DataType::Float64),
        ("string", Some("binary")) => Ok(DataType::Binary),
        ("string", Some("date-time")) => {
            let precision = obj.get("x-precision").and_then(|v| v.as_str());
            match precision {
                Some("micros") => Ok(DataType::Timestamp(
                    TimeUnit::Microsecond,
                    Some("UTC".into()),
                )),
                _ => Ok(DataType::Timestamp(
                    TimeUnit::Millisecond,
                    Some("UTC".into()),
                )),
            }
        }
        ("string", Some("date")) => Ok(DataType::Date32),
        ("string", Some("time")) => Ok(DataType::Time32(TimeUnit::Millisecond)),
        ("string", Some("decimal")) => {
            let precision = obj
                .get("x-precision")
                .and_then(|v| v.as_u64())
                .unwrap_or(38) as u8;
            let scale = obj
                .get("x-scale")
                .and_then(|v| v.as_i64())
                .unwrap_or(0) as i8;
            Ok(DataType::Decimal128(precision, scale))
        }
        ("string", _) => Ok(DataType::Utf8),
        ("array", _) => {
            let items = obj
                .get("items")
                .ok_or_else(|| LexiconError::InvalidSchema("array missing items".into()))?;
            let item_type = json_prop_to_arrow_type(items)?;
            Ok(DataType::List(Arc::new(Field::new(
                "item", item_type, true,
            ))))
        }
        ("object", _) => {
            if let Some(additional) = obj.get("additionalProperties") {
                // map 类型
                if additional.is_boolean() {
                    // additionalProperties: false 表示不允许额外属性，这不是 map
                    return Err(LexiconError::InvalidSchema(
                        "object without properties or additionalProperties schema".into(),
                    ));
                }
                let value_type = json_prop_to_arrow_type(additional)?;
                Ok(DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("key", DataType::Utf8, false),
                                Field::new("value", value_type, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ))
            } else if let Some(props) = obj.get("properties") {
                // struct 类型
                let props_obj = props
                    .as_object()
                    .ok_or_else(|| LexiconError::InvalidSchema("properties must be object".into()))?;
                let sub_required: Vec<String> = obj
                    .get("required")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default();
                let mut fields = Vec::new();
                for (k, v) in props_obj {
                    let nullable = !sub_required.contains(k);
                    let dt = json_prop_to_arrow_type(v)?;
                    fields.push(Field::new(k, dt, nullable));
                }
                Ok(DataType::Struct(fields.into()))
            } else {
                Err(LexiconError::InvalidSchema(
                    "object must have properties or additionalProperties".into(),
                ))
            }
        }
        _ => Err(LexiconError::InvalidSchema(format!(
            "unsupported JSON Schema type: {type_str}"
        ))),
    }
}

/// Avro Schema string → Arrow SchemaRef
pub fn avro_schema_to_arrow(avro_schema: &str) -> Result<SchemaRef> {
    let avro_schema_parsed = apache_avro::Schema::parse_str(avro_schema)
        .map_err(|e| LexiconError::InvalidSchema(format!("invalid Avro schema: {e}")))?;

    match &avro_schema_parsed {
        apache_avro::Schema::Record(record) => {
            let mut fields = Vec::new();
            for field in &record.fields {
                let (dt, nullable) = avro_type_to_arrow(&field.schema)?;
                fields.push(Field::new(&field.name, dt, nullable));
            }
            Ok(Arc::new(Schema::new(fields)))
        }
        _ => Err(LexiconError::InvalidSchema(
            "Avro schema must be a record".into(),
        )),
    }
}

/// Avro type → (Arrow DataType, nullable)
fn avro_type_to_arrow(schema: &apache_avro::Schema) -> Result<(DataType, bool)> {
    match schema {
        apache_avro::Schema::Null => Ok((DataType::Null, true)),
        apache_avro::Schema::Boolean => Ok((DataType::Boolean, false)),
        apache_avro::Schema::Int => Ok((DataType::Int32, false)),
        apache_avro::Schema::Long => Ok((DataType::Int64, false)),
        apache_avro::Schema::Float => Ok((DataType::Float32, false)),
        apache_avro::Schema::Double => Ok((DataType::Float64, false)),
        apache_avro::Schema::Bytes => Ok((DataType::Binary, false)),
        apache_avro::Schema::String => Ok((DataType::Utf8, false)),
        apache_avro::Schema::Union(union_schema) => {
            // Avro union: 常见的 ["null", "T"] 模式 → nullable T
            let variants = union_schema.variants();
            if variants.len() == 2 {
                let (null_idx, other_idx) = if matches!(variants[0], apache_avro::Schema::Null) {
                    (0, 1)
                } else if matches!(variants[1], apache_avro::Schema::Null) {
                    (1, 0)
                } else {
                    return Err(LexiconError::InvalidSchema(
                        "unsupported union type (not nullable)".into(),
                    ));
                };
                let _ = null_idx;
                let (dt, _) = avro_type_to_arrow(&variants[other_idx])?;
                Ok((dt, true))
            } else {
                Err(LexiconError::InvalidSchema(
                    "only nullable unions (2 variants with null) are supported".into(),
                ))
            }
        }
        apache_avro::Schema::Array(inner) => {
            let (item_dt, nullable) = avro_type_to_arrow(&inner.items)?;
            Ok((
                DataType::List(Arc::new(Field::new("item", item_dt, nullable))),
                false,
            ))
        }
        apache_avro::Schema::Map(inner) => {
            let (value_dt, nullable) = avro_type_to_arrow(&inner.types)?;
            Ok((
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("key", DataType::Utf8, false),
                                Field::new("value", value_dt, nullable),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                false,
            ))
        }
        apache_avro::Schema::Record(record) => {
            let mut fields = Vec::new();
            for field in &record.fields {
                let (dt, nullable) = avro_type_to_arrow(&field.schema)?;
                fields.push(Field::new(&field.name, dt, nullable));
            }
            Ok((DataType::Struct(fields.into()), false))
        }
        apache_avro::Schema::Enum(_) => Ok((
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )),
        apache_avro::Schema::Fixed(fixed) => {
            Ok((DataType::FixedSizeBinary(fixed.size as i32), false))
        }
        apache_avro::Schema::Date => Ok((DataType::Date32, false)),
        apache_avro::Schema::TimestampMillis => Ok((
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        )),
        apache_avro::Schema::TimestampMicros => Ok((
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        )),
        apache_avro::Schema::Decimal(decimal) => Ok((
            DataType::Decimal128(decimal.precision as u8, decimal.scale as i8),
            false,
        )),
        apache_avro::Schema::Duration => Ok((DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano), false)),
        apache_avro::Schema::TimeMillis => Ok((DataType::Time32(TimeUnit::Millisecond), false)),
        apache_avro::Schema::TimeMicros => Ok((DataType::Time64(TimeUnit::Microsecond), false)),
        _ => Err(LexiconError::InvalidSchema(format!(
            "unsupported Avro type: {schema:?}"
        ))),
    }
}

/// Protobuf descriptor bytes + message name → Arrow SchemaRef
pub fn protobuf_to_arrow(descriptor: &[u8], message_name: &str) -> Result<SchemaRef> {
    let file_desc = prost_types::FileDescriptorProto::decode(descriptor)
        .map_err(|e| LexiconError::InvalidSchema(format!("invalid protobuf descriptor: {e}")))?;

    let msg = file_desc
        .message_type
        .iter()
        .find(|m| m.name.as_deref() == Some(message_name))
        .ok_or_else(|| {
            LexiconError::InvalidSchema(format!("message {message_name} not found in descriptor"))
        })?;

    let fields = proto_message_to_arrow_fields(msg, &file_desc)?;
    Ok(Arc::new(Schema::new(fields)))
}

/// Protobuf message → Arrow fields
fn proto_message_to_arrow_fields(
    msg: &prost_types::DescriptorProto,
    file_desc: &prost_types::FileDescriptorProto,
) -> Result<Vec<Field>> {
    let mut fields = Vec::new();

    for field in &msg.field {
        let name = field.name.as_deref().unwrap_or("unknown");
        let label = field.label();

        let is_repeated = label == prost_types::field_descriptor_proto::Label::Repeated;
        let nullable = label == prost_types::field_descriptor_proto::Label::Optional;

        let dt = proto_field_to_arrow_type(field, msg, file_desc)?;
        let final_dt = if is_repeated {
            DataType::List(Arc::new(Field::new("item", dt, true)))
        } else {
            dt
        };

        fields.push(Field::new(name, final_dt, nullable || is_repeated));
    }

    Ok(fields)
}

/// 单个 protobuf field → Arrow DataType
fn proto_field_to_arrow_type(
    field: &prost_types::FieldDescriptorProto,
    parent_msg: &prost_types::DescriptorProto,
    file_desc: &prost_types::FileDescriptorProto,
) -> Result<DataType> {
    use prost_types::field_descriptor_proto::Type;

    match field.r#type() {
        Type::Double => Ok(DataType::Float64),
        Type::Float => Ok(DataType::Float32),
        Type::Int64 | Type::Sint64 | Type::Sfixed64 => Ok(DataType::Int64),
        Type::Uint64 | Type::Fixed64 => Ok(DataType::UInt64),
        Type::Int32 | Type::Sint32 | Type::Sfixed32 => Ok(DataType::Int32),
        Type::Uint32 | Type::Fixed32 => Ok(DataType::UInt32),
        Type::Bool => Ok(DataType::Boolean),
        Type::String => Ok(DataType::Utf8),
        Type::Bytes => Ok(DataType::Binary),
        Type::Enum => Ok(DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8),
        )),
        Type::Message => {
            // 查找嵌套 message
            let type_name = field.type_name.as_deref().unwrap_or("");
            let msg_name = type_name.rsplit('.').next().unwrap_or(type_name);

            // 先查嵌套类型
            if let Some(nested) = parent_msg
                .nested_type
                .iter()
                .find(|m| m.name.as_deref() == Some(msg_name))
            {
                // 检查是否是 map entry
                if nested.options.as_ref().is_some_and(|o| o.map_entry()) {
                    let key_field = nested.field.iter().find(|f| f.name.as_deref() == Some("key"));
                    let value_field = nested.field.iter().find(|f| f.name.as_deref() == Some("value"));
                    if let (Some(kf), Some(vf)) = (key_field, value_field) {
                        let key_dt = proto_field_to_arrow_type(kf, nested, file_desc)?;
                        let val_dt = proto_field_to_arrow_type(vf, nested, file_desc)?;
                        return Ok(DataType::Map(
                            Arc::new(Field::new(
                                "entries",
                                DataType::Struct(
                                    vec![
                                        Field::new("key", key_dt, false),
                                        Field::new("value", val_dt, true),
                                    ]
                                    .into(),
                                ),
                                false,
                            )),
                            false,
                        ));
                    }
                }
                let fields = proto_message_to_arrow_fields(nested, file_desc)?;
                return Ok(DataType::Struct(fields.into()));
            }

            // 查顶层 message
            if let Some(top_msg) = file_desc
                .message_type
                .iter()
                .find(|m| m.name.as_deref() == Some(msg_name))
            {
                let fields = proto_message_to_arrow_fields(top_msg, file_desc)?;
                return Ok(DataType::Struct(fields.into()));
            }

            Err(LexiconError::InvalidSchema(format!(
                "message type not found: {msg_name}"
            )))
        }
        Type::Group => Err(LexiconError::InvalidSchema(
            "protobuf groups are not supported".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_schema_all_basic_types() {
        let schema = r#"{
            "type": "object",
            "properties": {
                "flag": {"type": "boolean"},
                "count32": {"type": "integer", "format": "int32"},
                "count64": {"type": "integer", "format": "int64"},
                "ratio": {"type": "number", "format": "float"},
                "value": {"type": "number", "format": "double"},
                "name": {"type": "string"},
                "data": {"type": "string", "format": "binary", "contentEncoding": "base64"},
                "ts_ms": {"type": "string", "format": "date-time", "x-precision": "millis"},
                "ts_us": {"type": "string", "format": "date-time", "x-precision": "micros"},
                "day": {"type": "string", "format": "date"},
                "time": {"type": "string", "format": "time"},
                "amount": {"type": "string", "format": "decimal", "x-precision": 10, "x-scale": 2},
                "status": {"type": "string", "enum": ["a", "b"]}
            },
            "required": ["count64"]
        }"#;

        let arrow = json_schema_to_arrow(schema).unwrap();
        assert_eq!(arrow.fields().len(), 13);

        // 检查具体类型
        let field = arrow.field_with_name("flag").unwrap();
        assert_eq!(field.data_type(), &DataType::Boolean);
        assert!(field.is_nullable());

        let field = arrow.field_with_name("count64").unwrap();
        assert_eq!(field.data_type(), &DataType::Int64);
        assert!(!field.is_nullable());

        let field = arrow.field_with_name("amount").unwrap();
        assert_eq!(field.data_type(), &DataType::Decimal128(10, 2));

        let field = arrow.field_with_name("status").unwrap();
        assert!(matches!(field.data_type(), DataType::Dictionary(..)));
    }

    #[test]
    fn test_json_schema_nested() {
        let schema = r#"{
            "type": "object",
            "properties": {
                "address": {
                    "type": "object",
                    "properties": {
                        "city": {"type": "string"},
                        "zip": {"type": "string"}
                    }
                }
            }
        }"#;

        let arrow = json_schema_to_arrow(schema).unwrap();
        let field = arrow.field_with_name("address").unwrap();
        match field.data_type() {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
            }
            _ => panic!("expected Struct"),
        }
    }

    #[test]
    fn test_json_schema_list() {
        let schema = r#"{
            "type": "object",
            "properties": {
                "tags": {
                    "type": "array",
                    "items": {"type": "string"}
                }
            }
        }"#;

        let arrow = json_schema_to_arrow(schema).unwrap();
        let field = arrow.field_with_name("tags").unwrap();
        match field.data_type() {
            DataType::List(inner) => {
                assert_eq!(inner.data_type(), &DataType::Utf8);
            }
            _ => panic!("expected List"),
        }
    }

    #[test]
    fn test_json_schema_map() {
        let schema = r#"{
            "type": "object",
            "properties": {
                "metadata": {
                    "type": "object",
                    "additionalProperties": {"type": "string"}
                }
            }
        }"#;

        let arrow = json_schema_to_arrow(schema).unwrap();
        let field = arrow.field_with_name("metadata").unwrap();
        assert!(matches!(field.data_type(), DataType::Map(..)));
    }

    #[test]
    fn test_avro_to_arrow_basic() {
        let avro = r#"{
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "active", "type": "boolean"},
                {"name": "score", "type": "double"}
            ]
        }"#;

        let arrow = avro_schema_to_arrow(avro).unwrap();
        assert_eq!(arrow.fields().len(), 4);

        let field = arrow.field_with_name("id").unwrap();
        assert_eq!(field.data_type(), &DataType::Int64);
    }

    #[test]
    fn test_avro_logical_types() {
        let avro = r#"{
            "type": "record",
            "name": "Test",
            "fields": [
                {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "d", "type": {"type": "int", "logicalType": "date"}},
                {"name": "nullable_name", "type": ["null", "string"]}
            ]
        }"#;

        let arrow = avro_schema_to_arrow(avro).unwrap();

        let field = arrow.field_with_name("ts").unwrap();
        assert!(matches!(
            field.data_type(),
            DataType::Timestamp(TimeUnit::Millisecond, _)
        ));

        let field = arrow.field_with_name("d").unwrap();
        assert_eq!(field.data_type(), &DataType::Date32);

        let field = arrow.field_with_name("nullable_name").unwrap();
        assert_eq!(field.data_type(), &DataType::Utf8);
        assert!(field.is_nullable());
    }

    #[test]
    fn test_protobuf_to_arrow_basic() {
        use prost::Message;

        // 构造一个简单的 FileDescriptorProto
        let msg = prost_types::DescriptorProto {
            name: Some("TestMessage".to_string()),
            field: vec![
                prost_types::FieldDescriptorProto {
                    name: Some("id".to_string()),
                    number: Some(1),
                    r#type: Some(prost_types::field_descriptor_proto::Type::Int64 as i32),
                    label: Some(
                        prost_types::field_descriptor_proto::Label::Optional as i32,
                    ),
                    ..Default::default()
                },
                prost_types::FieldDescriptorProto {
                    name: Some("name".to_string()),
                    number: Some(2),
                    r#type: Some(prost_types::field_descriptor_proto::Type::String as i32),
                    label: Some(
                        prost_types::field_descriptor_proto::Label::Optional as i32,
                    ),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let file_desc = prost_types::FileDescriptorProto {
            message_type: vec![msg],
            ..Default::default()
        };

        let bytes = file_desc.encode_to_vec();
        let arrow = protobuf_to_arrow(&bytes, "TestMessage").unwrap();

        assert_eq!(arrow.fields().len(), 2);
        let field = arrow.field_with_name("id").unwrap();
        assert_eq!(field.data_type(), &DataType::Int64);
    }
}
