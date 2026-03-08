//! Avro Forward Compatibility 检查
//!
//! Avro 规则：新 schema 能读旧 schema 写的数据
//! - 新增有默认值的字段 → OK
//! - 删除没有默认值的字段 → FAIL
//! - 修改字段类型（除了 int→long, float→double 等 promotion）→ FAIL
//! - enum 添加 variant → OK（如果有默认值）

use apache_avro::Schema;

use crate::error::{LexiconError, Result};

/// 检查 Avro forward compatibility
///
/// 返回 (compatible, violations)
pub fn check(old_avro: &str, new_avro: &str) -> Result<(bool, Vec<String>)> {
    let old_schema = Schema::parse_str(old_avro)
        .map_err(|e| LexiconError::InvalidSchema(format!("invalid old Avro schema: {e}")))?;
    let new_schema = Schema::parse_str(new_avro)
        .map_err(|e| LexiconError::InvalidSchema(format!("invalid new Avro schema: {e}")))?;

    let mut violations = Vec::new();

    match (&old_schema, &new_schema) {
        (Schema::Record(old_rec), Schema::Record(new_rec)) => {
            check_record_compat(old_rec, new_rec, "", &mut violations);
        }
        _ => {
            violations.push("both schemas must be records".to_string());
        }
    }

    Ok((violations.is_empty(), violations))
}

/// 递归检查 record 兼容性
fn check_record_compat(
    old_rec: &apache_avro::schema::RecordSchema,
    new_rec: &apache_avro::schema::RecordSchema,
    path: &str,
    violations: &mut Vec<String>,
) {
    // 检查删除的字段
    for old_field in &old_rec.fields {
        let field_path = if path.is_empty() {
            old_field.name.clone()
        } else {
            format!("{path}.{}", old_field.name)
        };

        match new_rec.fields.iter().find(|f| f.name == old_field.name) {
            None => {
                // 字段被删除，检查旧字段是否有默认值
                if old_field.default.is_none() {
                    violations.push(format!(
                        "{field_path}: field removed without default value"
                    ));
                }
            }
            Some(new_field) => {
                // 检查类型兼容性
                check_type_compat(&old_field.schema, &new_field.schema, &field_path, violations);
            }
        }
    }

    // 检查新增字段
    for new_field in &new_rec.fields {
        let field_path = if path.is_empty() {
            new_field.name.clone()
        } else {
            format!("{path}.{}", new_field.name)
        };

        if !old_rec.fields.iter().any(|f| f.name == new_field.name) {
            // 新增字段必须有默认值或者是 nullable (union with null)
            let has_default = new_field.default.is_some();
            let is_nullable = is_nullable_union(&new_field.schema);
            if !has_default && !is_nullable {
                violations.push(format!(
                    "{field_path}: new field must have a default value or be nullable"
                ));
            }
        }
    }
}

/// 检查类型兼容性
fn check_type_compat(
    old: &Schema,
    new: &Schema,
    path: &str,
    violations: &mut Vec<String>,
) {
    // 解包 nullable union
    let old_inner = unwrap_nullable(old);
    let new_inner = unwrap_nullable(new);

    match (old_inner, new_inner) {
        (Schema::Record(old_rec), Schema::Record(new_rec)) => {
            check_record_compat(old_rec, new_rec, path, violations);
        }
        (Schema::Array(old_arr), Schema::Array(new_arr)) => {
            check_type_compat(&old_arr.items, &new_arr.items, &format!("{path}.<items>"), violations);
        }
        (Schema::Map(old_map), Schema::Map(new_map)) => {
            check_type_compat(&old_map.types, &new_map.types, &format!("{path}.<values>"), violations);
        }
        (Schema::Enum(old_enum), Schema::Enum(new_enum)) => {
            // enum: 不能删除已有 symbol
            for symbol in &old_enum.symbols {
                if !new_enum.symbols.contains(symbol) {
                    violations.push(format!("{path}: enum symbol '{symbol}' removed"));
                }
            }
        }
        _ => {
            // 基本类型检查
            if !is_type_promotable(old_inner, new_inner) {
                let old_name = schema_type_name(old_inner);
                let new_name = schema_type_name(new_inner);
                if old_name != new_name {
                    violations.push(format!(
                        "{path}: type changed from {old_name} to {new_name}"
                    ));
                }
            }
        }
    }
}

/// 检查是否可以类型提升
fn is_type_promotable(old: &Schema, new: &Schema) -> bool {
    matches!(
        (old, new),
        (Schema::Int, Schema::Long)
            | (Schema::Int, Schema::Float)
            | (Schema::Int, Schema::Double)
            | (Schema::Long, Schema::Float)
            | (Schema::Long, Schema::Double)
            | (Schema::Float, Schema::Double)
            | (Schema::String, Schema::Bytes)
            | (Schema::Bytes, Schema::String)
    )
}

/// 检查是否是 nullable union
fn is_nullable_union(schema: &Schema) -> bool {
    if let Schema::Union(union_schema) = schema {
        let variants = union_schema.variants();
        variants.len() == 2 && variants.iter().any(|v| matches!(v, Schema::Null))
    } else {
        false
    }
}

/// 解包 nullable union → 内部非 null 类型
fn unwrap_nullable(schema: &Schema) -> &Schema {
    if let Schema::Union(union_schema) = schema {
        let variants = union_schema.variants();
        if variants.len() == 2 {
            for v in variants {
                if !matches!(v, Schema::Null) {
                    return v;
                }
            }
        }
    }
    schema
}

/// 获取 schema 类型名称
fn schema_type_name(schema: &Schema) -> &'static str {
    match schema {
        Schema::Null => "null",
        Schema::Boolean => "boolean",
        Schema::Int => "int",
        Schema::Long => "long",
        Schema::Float => "float",
        Schema::Double => "double",
        Schema::Bytes => "bytes",
        Schema::String => "string",
        Schema::Array(_) => "array",
        Schema::Map(_) => "map",
        Schema::Union(_) => "union",
        Schema::Record(_) => "record",
        Schema::Enum(_) => "enum",
        Schema::Fixed(_) => "fixed",
        Schema::Date => "date",
        Schema::TimestampMillis => "timestamp-millis",
        Schema::TimestampMicros => "timestamp-micros",
        Schema::Decimal(_) => "decimal",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record_schema(fields: &str) -> String {
        format!(
            r#"{{"type":"record","name":"Test","fields":[{fields}]}}"#
        )
    }

    #[test]
    fn test_add_field_with_default_ok() {
        let old = record_schema(r#"{"name":"id","type":"long"}"#);
        let new = record_schema(
            r#"{"name":"id","type":"long"},{"name":"name","type":"string","default":""}"#,
        );
        let (ok, violations) = check(&old, &new).unwrap();
        assert!(ok, "violations: {violations:?}");
    }

    #[test]
    fn test_add_nullable_field_ok() {
        let old = record_schema(r#"{"name":"id","type":"long"}"#);
        let new = record_schema(
            r#"{"name":"id","type":"long"},{"name":"name","type":["null","string"],"default":null}"#,
        );
        let (ok, violations) = check(&old, &new).unwrap();
        assert!(ok, "violations: {violations:?}");
    }

    #[test]
    fn test_add_required_field_fail() {
        let old = record_schema(r#"{"name":"id","type":"long"}"#);
        let new = record_schema(
            r#"{"name":"id","type":"long"},{"name":"name","type":"string"}"#,
        );
        let (ok, violations) = check(&old, &new).unwrap();
        assert!(!ok);
        assert!(violations.iter().any(|v| v.contains("default value") || v.contains("nullable")));
    }

    #[test]
    fn test_remove_field_without_default_fail() {
        let old = record_schema(
            r#"{"name":"id","type":"long"},{"name":"name","type":"string"}"#,
        );
        let new = record_schema(r#"{"name":"id","type":"long"}"#);
        let (ok, violations) = check(&old, &new).unwrap();
        assert!(!ok);
        assert!(violations.iter().any(|v| v.contains("removed")));
    }

    #[test]
    fn test_remove_field_with_default_ok() {
        let old = record_schema(
            r#"{"name":"id","type":"long"},{"name":"name","type":"string","default":""}"#,
        );
        let new = record_schema(r#"{"name":"id","type":"long"}"#);
        let (ok, violations) = check(&old, &new).unwrap();
        assert!(ok, "violations: {violations:?}");
    }

    #[test]
    fn test_type_change_fail() {
        let old = record_schema(r#"{"name":"id","type":"long"}"#);
        let new = record_schema(r#"{"name":"id","type":"string"}"#);
        let (ok, violations) = check(&old, &new).unwrap();
        assert!(!ok);
        assert!(violations.iter().any(|v| v.contains("type changed")));
    }

    #[test]
    fn test_type_promotion_ok() {
        let old = record_schema(r#"{"name":"id","type":"int"}"#);
        let new = record_schema(r#"{"name":"id","type":"long"}"#);
        let (ok, violations) = check(&old, &new).unwrap();
        assert!(ok, "violations: {violations:?}");
    }

    #[test]
    fn test_enum_add_symbol_ok() {
        let old = record_schema(
            r#"{"name":"status","type":{"type":"enum","name":"Status","symbols":["A","B"]}}"#,
        );
        let new = record_schema(
            r#"{"name":"status","type":{"type":"enum","name":"Status","symbols":["A","B","C"]}}"#,
        );
        let (ok, violations) = check(&old, &new).unwrap();
        assert!(ok, "violations: {violations:?}");
    }

    #[test]
    fn test_enum_remove_symbol_fail() {
        let old = record_schema(
            r#"{"name":"status","type":{"type":"enum","name":"Status","symbols":["A","B","C"]}}"#,
        );
        let new = record_schema(
            r#"{"name":"status","type":{"type":"enum","name":"Status","symbols":["A","B"]}}"#,
        );
        let (ok, violations) = check(&old, &new).unwrap();
        assert!(!ok);
        assert!(violations.iter().any(|v| v.contains("symbol") && v.contains("removed")));
    }
}
