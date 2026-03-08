//! 基础类型 fields 数组 → JSON Schema 转换
//!
//! 这是 Lexicon 的核心语法糖：用户使用简单的 fields 描述，
//! Lexicon 自动转换为标准 JSON Schema 存储。

use serde_json::{Map, Value, json};

use crate::error::{LexiconError, Result};
use crate::types::{BasicField, BasicType, BasicTypeOrRef};

/// 将用户提交的 BasicField 数组转换为 JSON Schema string
pub fn basic_fields_to_json_schema(fields: &[BasicField]) -> Result<String> {
    let (properties, required) = fields_to_properties(fields)?;

    let mut schema = Map::new();
    schema.insert("type".to_string(), json!("object"));
    schema.insert("properties".to_string(), Value::Object(properties));
    if !required.is_empty() {
        schema.insert("required".to_string(), json!(required));
    }
    schema.insert("additionalProperties".to_string(), json!(false));

    serde_json::to_string(&Value::Object(schema)).map_err(|e| LexiconError::Serialization(e.to_string()))
}

/// 递归构建 properties 和 required 数组
fn fields_to_properties(fields: &[BasicField]) -> Result<(Map<String, Value>, Vec<String>)> {
    let mut properties = Map::new();
    let mut required = Vec::new();

    for field in fields {
        let prop = basic_type_to_json_schema(&field.field_type, field)?;
        properties.insert(field.name.clone(), prop);
        if !field.nullable {
            required.push(field.name.clone());
        }
    }

    Ok((properties, required))
}

/// 单个基础类型 → JSON Schema property
fn basic_type_to_json_schema(basic_type: &BasicType, field: &BasicField) -> Result<Value> {
    match basic_type {
        BasicType::Bool => Ok(json!({"type": "boolean"})),
        BasicType::Int32 => Ok(json!({"type": "integer", "format": "int32"})),
        BasicType::Int64 => Ok(json!({"type": "integer", "format": "int64"})),
        BasicType::Float32 => Ok(json!({"type": "number", "format": "float"})),
        BasicType::Float64 => Ok(json!({"type": "number", "format": "double"})),
        BasicType::String => Ok(json!({"type": "string"})),
        BasicType::Binary => Ok(json!({"type": "string", "format": "binary", "contentEncoding": "base64"})),
        BasicType::TimestampMs => Ok(json!({"type": "string", "format": "date-time", "x-precision": "millis"})),
        BasicType::TimestampUs => Ok(json!({"type": "string", "format": "date-time", "x-precision": "micros"})),
        BasicType::Date => Ok(json!({"type": "string", "format": "date"})),
        BasicType::TimeMs => Ok(json!({"type": "string", "format": "time"})),
        BasicType::Decimal => {
            let precision = field
                .precision
                .ok_or_else(|| LexiconError::InvalidBasicType("decimal requires precision".into()))?;
            let scale = field
                .scale
                .ok_or_else(|| LexiconError::InvalidBasicType("decimal requires scale".into()))?;
            Ok(json!({
                "type": "string",
                "format": "decimal",
                "x-precision": precision,
                "x-scale": scale
            }))
        }
        BasicType::Enum => {
            let variants = field
                .variants
                .as_ref()
                .ok_or_else(|| LexiconError::InvalidBasicType("enum requires variants".into()))?;
            Ok(json!({"type": "string", "enum": variants}))
        }
        BasicType::List => {
            let element = field
                .element
                .as_ref()
                .ok_or_else(|| LexiconError::InvalidBasicType("list requires element type".into()))?;
            let items = type_ref_to_json_schema(element)?;
            Ok(json!({"type": "array", "items": items}))
        }
        BasicType::Map => {
            let value = field
                .value
                .as_ref()
                .ok_or_else(|| LexiconError::InvalidBasicType("map requires value type".into()))?;
            let additional = type_ref_to_json_schema(value)?;
            Ok(json!({"type": "object", "additionalProperties": additional}))
        }
        BasicType::Struct => {
            let sub_fields = field
                .fields
                .as_ref()
                .ok_or_else(|| LexiconError::InvalidBasicType("struct requires fields".into()))?;
            let (properties, required) = fields_to_properties(sub_fields)?;
            let mut obj = Map::new();
            obj.insert("type".to_string(), json!("object"));
            obj.insert("properties".to_string(), Value::Object(properties));
            if !required.is_empty() {
                obj.insert("required".to_string(), json!(required));
            }
            Ok(Value::Object(obj))
        }
    }
}

/// BasicTypeOrRef → JSON Schema
fn type_ref_to_json_schema(type_ref: &BasicTypeOrRef) -> Result<Value> {
    match type_ref {
        BasicTypeOrRef::Simple { field_type } => {
            // 简单类型引用，构造一个临时 BasicField
            let dummy_field = BasicField {
                name: String::new(),
                field_type: field_type.clone(),
                nullable: true,
                element: None,
                key: None,
                value: None,
                fields: None,
                variants: None,
                precision: None,
                scale: None,
            };
            basic_type_to_json_schema(field_type, &dummy_field)
        }
        BasicTypeOrRef::Full(field) => basic_type_to_json_schema(&field.field_type, field),
    }
}

/// 反向转换: JSON Schema → BasicField 数组（用于 API 展示）
pub fn json_schema_to_basic_fields(json_schema: &str) -> Result<Vec<BasicField>> {
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
        let field = json_prop_to_basic_field(name, prop, &required)?;
        fields.push(field);
    }

    Ok(fields)
}

/// 单个 JSON Schema property → BasicField
fn json_prop_to_basic_field(name: &str, prop: &Value, required: &[String]) -> Result<BasicField> {
    let obj = prop
        .as_object()
        .ok_or_else(|| LexiconError::InvalidSchema(format!("property {name} must be object")))?;

    let nullable = !required.contains(&name.to_string());

    // 检查 enum
    if let Some(variants) = obj.get("enum") {
        let variants: Vec<String> = variants
            .as_array()
            .ok_or_else(|| LexiconError::InvalidSchema("enum must be array".into()))?
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        return Ok(BasicField {
            name: name.to_string(),
            field_type: BasicType::Enum,
            nullable,
            element: None,
            key: None,
            value: None,
            fields: None,
            variants: Some(variants),
            precision: None,
            scale: None,
        });
    }

    let type_str = obj
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| LexiconError::InvalidSchema(format!("property {name} missing type")))?;

    let format_str = obj.get("format").and_then(|v| v.as_str());

    match (type_str, format_str) {
        ("boolean", _) => Ok(make_simple_field(name, BasicType::Bool, nullable)),
        ("integer", Some("int32")) => Ok(make_simple_field(name, BasicType::Int32, nullable)),
        ("integer", _) => Ok(make_simple_field(name, BasicType::Int64, nullable)),
        ("number", Some("float")) => Ok(make_simple_field(name, BasicType::Float32, nullable)),
        ("number", _) => Ok(make_simple_field(name, BasicType::Float64, nullable)),
        ("string", Some("binary")) => Ok(make_simple_field(name, BasicType::Binary, nullable)),
        ("string", Some("date-time")) => {
            let precision = obj.get("x-precision").and_then(|v| v.as_str());
            let bt = match precision {
                Some("micros") => BasicType::TimestampUs,
                _ => BasicType::TimestampMs,
            };
            Ok(make_simple_field(name, bt, nullable))
        }
        ("string", Some("date")) => Ok(make_simple_field(name, BasicType::Date, nullable)),
        ("string", Some("time")) => Ok(make_simple_field(name, BasicType::TimeMs, nullable)),
        ("string", Some("decimal")) => {
            let precision = obj
                .get("x-precision")
                .and_then(|v| v.as_u64())
                .map(|v| v as u8);
            let scale = obj
                .get("x-scale")
                .and_then(|v| v.as_i64())
                .map(|v| v as i8);
            Ok(BasicField {
                name: name.to_string(),
                field_type: BasicType::Decimal,
                nullable,
                element: None,
                key: None,
                value: None,
                fields: None,
                variants: None,
                precision,
                scale,
            })
        }
        ("string", _) => Ok(make_simple_field(name, BasicType::String, nullable)),
        ("array", _) => {
            let items = obj
                .get("items")
                .ok_or_else(|| LexiconError::InvalidSchema("array missing items".into()))?;
            let element = json_value_to_type_ref(items)?;
            Ok(BasicField {
                name: name.to_string(),
                field_type: BasicType::List,
                nullable,
                element: Some(Box::new(element)),
                key: None,
                value: None,
                fields: None,
                variants: None,
                precision: None,
                scale: None,
            })
        }
        ("object", _) => {
            if let Some(additional) = obj.get("additionalProperties") {
                // map 类型
                let value_ref = json_value_to_type_ref(additional)?;
                Ok(BasicField {
                    name: name.to_string(),
                    field_type: BasicType::Map,
                    nullable,
                    element: None,
                    key: Some(Box::new(BasicTypeOrRef::Simple {
                        field_type: BasicType::String,
                    })),
                    value: Some(Box::new(value_ref)),
                    fields: None,
                    variants: None,
                    precision: None,
                    scale: None,
                })
            } else if let Some(props) = obj.get("properties") {
                // struct 类型
                let sub_required: Vec<String> = obj
                    .get("required")
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect()
                    })
                    .unwrap_or_default();
                let props_obj = props
                    .as_object()
                    .ok_or_else(|| LexiconError::InvalidSchema("properties must be object".into()))?;
                let mut sub_fields = Vec::new();
                for (k, v) in props_obj {
                    sub_fields.push(json_prop_to_basic_field(k, v, &sub_required)?);
                }
                Ok(BasicField {
                    name: name.to_string(),
                    field_type: BasicType::Struct,
                    nullable,
                    element: None,
                    key: None,
                    value: None,
                    fields: Some(sub_fields),
                    variants: None,
                    precision: None,
                    scale: None,
                })
            } else {
                Err(LexiconError::InvalidSchema(format!(
                    "object property {name} must have properties or additionalProperties"
                )))
            }
        }
        _ => Err(LexiconError::InvalidSchema(format!(
            "unsupported type: {type_str}"
        ))),
    }
}

fn make_simple_field(name: &str, field_type: BasicType, nullable: bool) -> BasicField {
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

/// JSON Schema value → BasicTypeOrRef
fn json_value_to_type_ref(value: &Value) -> Result<BasicTypeOrRef> {
    let obj = value
        .as_object()
        .ok_or_else(|| LexiconError::InvalidSchema("type ref must be object".into()))?;

    // 检查 enum
    if obj.contains_key("enum") {
        let field = json_prop_to_basic_field("_", value, &[])?;
        return Ok(BasicTypeOrRef::Full(field));
    }

    let type_str = obj
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| LexiconError::InvalidSchema("type ref missing type".into()))?;

    let format_str = obj.get("format").and_then(|v| v.as_str());

    // 复杂类型返回 Full
    if type_str == "array" || type_str == "object" {
        let field = json_prop_to_basic_field("_", value, &[])?;
        return Ok(BasicTypeOrRef::Full(field));
    }

    // 简单类型
    let bt = match (type_str, format_str) {
        ("boolean", _) => BasicType::Bool,
        ("integer", Some("int32")) => BasicType::Int32,
        ("integer", _) => BasicType::Int64,
        ("number", Some("float")) => BasicType::Float32,
        ("number", _) => BasicType::Float64,
        ("string", Some("binary")) => BasicType::Binary,
        ("string", Some("date-time")) => {
            let precision = obj.get("x-precision").and_then(|v| v.as_str());
            match precision {
                Some("micros") => BasicType::TimestampUs,
                _ => BasicType::TimestampMs,
            }
        }
        ("string", Some("date")) => BasicType::Date,
        ("string", Some("time")) => BasicType::TimeMs,
        ("string", Some("decimal")) => {
            let field = json_prop_to_basic_field("_", value, &[])?;
            return Ok(BasicTypeOrRef::Full(field));
        }
        ("string", _) => BasicType::String,
        _ => {
            return Err(LexiconError::InvalidSchema(format!(
                "unsupported type ref: {type_str}"
            )))
        }
    };

    Ok(BasicTypeOrRef::Simple { field_type: bt })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_types_roundtrip() {
        let fields = vec![
            make_simple_field("id", BasicType::Int64, false),
            make_simple_field("name", BasicType::String, true),
            make_simple_field("active", BasicType::Bool, true),
            make_simple_field("score", BasicType::Float64, true),
        ];

        let json_schema = basic_fields_to_json_schema(&fields).unwrap();
        let roundtrip = json_schema_to_basic_fields(&json_schema).unwrap();

        assert_eq!(roundtrip.len(), 4);
        // 检查 id 是非 nullable
        let id = roundtrip.iter().find(|f| f.name == "id").unwrap();
        assert!(!id.nullable);
        assert_eq!(id.field_type, BasicType::Int64);
    }

    #[test]
    fn test_nested_struct() {
        let fields = vec![BasicField {
            name: "address".to_string(),
            field_type: BasicType::Struct,
            nullable: true,
            element: None,
            key: None,
            value: None,
            fields: Some(vec![
                make_simple_field("city", BasicType::String, true),
                make_simple_field("zip", BasicType::String, true),
            ]),
            variants: None,
            precision: None,
            scale: None,
        }];

        let json_schema = basic_fields_to_json_schema(&fields).unwrap();
        let value: Value = serde_json::from_str(&json_schema).unwrap();

        // 验证嵌套 object
        let addr = &value["properties"]["address"];
        assert_eq!(addr["type"], "object");
        assert!(addr["properties"]["city"].is_object());
    }

    #[test]
    fn test_list_of_struct() {
        let fields = vec![BasicField {
            name: "items".to_string(),
            field_type: BasicType::List,
            nullable: true,
            element: Some(Box::new(BasicTypeOrRef::Full(BasicField {
                name: "_".to_string(),
                field_type: BasicType::Struct,
                nullable: true,
                element: None,
                key: None,
                value: None,
                fields: Some(vec![make_simple_field("name", BasicType::String, true)]),
                variants: None,
                precision: None,
                scale: None,
            }))),
            key: None,
            value: None,
            fields: None,
            variants: None,
            precision: None,
            scale: None,
        }];

        let json_schema = basic_fields_to_json_schema(&fields).unwrap();
        let value: Value = serde_json::from_str(&json_schema).unwrap();

        assert_eq!(value["properties"]["items"]["type"], "array");
        assert_eq!(value["properties"]["items"]["items"]["type"], "object");
    }

    #[test]
    fn test_map_type() {
        let fields = vec![BasicField {
            name: "metadata".to_string(),
            field_type: BasicType::Map,
            nullable: true,
            element: None,
            key: Some(Box::new(BasicTypeOrRef::Simple {
                field_type: BasicType::String,
            })),
            value: Some(Box::new(BasicTypeOrRef::Simple {
                field_type: BasicType::Int64,
            })),
            fields: None,
            variants: None,
            precision: None,
            scale: None,
        }];

        let json_schema = basic_fields_to_json_schema(&fields).unwrap();
        let value: Value = serde_json::from_str(&json_schema).unwrap();

        let meta = &value["properties"]["metadata"];
        assert_eq!(meta["type"], "object");
        assert_eq!(meta["additionalProperties"]["type"], "integer");
    }

    #[test]
    fn test_decimal_params() {
        let fields = vec![BasicField {
            name: "amount".to_string(),
            field_type: BasicType::Decimal,
            nullable: false,
            element: None,
            key: None,
            value: None,
            fields: None,
            variants: None,
            precision: Some(10),
            scale: Some(2),
        }];

        let json_schema = basic_fields_to_json_schema(&fields).unwrap();
        let value: Value = serde_json::from_str(&json_schema).unwrap();

        let amount = &value["properties"]["amount"];
        assert_eq!(amount["format"], "decimal");
        assert_eq!(amount["x-precision"], 10);
        assert_eq!(amount["x-scale"], 2);
    }

    #[test]
    fn test_enum_variants() {
        let fields = vec![BasicField {
            name: "status".to_string(),
            field_type: BasicType::Enum,
            nullable: true,
            element: None,
            key: None,
            value: None,
            fields: None,
            variants: Some(vec!["pending".into(), "paid".into(), "shipped".into()]),
            precision: None,
            scale: None,
        }];

        let json_schema = basic_fields_to_json_schema(&fields).unwrap();
        let value: Value = serde_json::from_str(&json_schema).unwrap();

        let status = &value["properties"]["status"];
        assert_eq!(status["type"], "string");
        let enums = status["enum"].as_array().unwrap();
        assert_eq!(enums.len(), 3);
        assert_eq!(enums[0], "pending");
    }
}
