//! JSON Schema Forward Compatibility 检查
//!
//! 规则（递归应用于嵌套 object）:
//! - 新增 property 且不在 required 中（nullable）→ OK
//! - 新增 property 且在 required 中（non-nullable）→ FAIL
//! - 删除已有 property → FAIL
//! - 修改已有 property 的 type 或 format → FAIL
//! - 将已有 property 加入 required → FAIL
//! - 将已有 property 从 required 移除 → OK
//! - enum 添加新 variant → OK
//! - enum 删除已有 variant → FAIL
//! - decimal precision 增大 → OK，减小 → FAIL，scale 不能变

use serde_json::Value;

use crate::error::{LexiconError, Result};

/// 检查 JSON Schema forward compatibility
///
/// 返回 (compatible, violations)
pub fn check(old_json: &str, new_json: &str) -> Result<(bool, Vec<String>)> {
    let old: Value = serde_json::from_str(old_json)
        .map_err(|e| LexiconError::InvalidSchema(format!("invalid old JSON Schema: {e}")))?;
    let new: Value = serde_json::from_str(new_json)
        .map_err(|e| LexiconError::InvalidSchema(format!("invalid new JSON Schema: {e}")))?;

    let mut violations = Vec::new();
    check_object_compat(&old, &new, "", &mut violations);

    Ok((violations.is_empty(), violations))
}

/// 递归检查 object 兼容性
fn check_object_compat(old: &Value, new: &Value, path: &str, violations: &mut Vec<String>) {
    let old_obj = match old.as_object() {
        Some(o) => o,
        None => return,
    };
    let new_obj = match new.as_object() {
        Some(o) => o,
        None => {
            violations.push(format!("{path}: schema changed from object to non-object"));
            return;
        }
    };

    let old_type = old_obj.get("type").and_then(|v| v.as_str());
    let new_type = new_obj.get("type").and_then(|v| v.as_str());

    // 类型变化检查
    if old_type != new_type {
        violations.push(format!(
            "{path}: type changed from {:?} to {:?}",
            old_type, new_type
        ));
        return;
    }

    // format 变化检查
    let old_format = old_obj.get("format").and_then(|v| v.as_str());
    let new_format = new_obj.get("format").and_then(|v| v.as_str());
    if old_format != new_format {
        violations.push(format!(
            "{path}: format changed from {:?} to {:?}",
            old_format, new_format
        ));
        return;
    }

    // enum 检查
    if let (Some(old_enum), Some(new_enum)) = (old_obj.get("enum"), new_obj.get("enum")) {
        check_enum_compat(old_enum, new_enum, path, violations);
        return;
    } else if old_obj.contains_key("enum") != new_obj.contains_key("enum") {
        violations.push(format!("{path}: enum constraint added or removed"));
        return;
    }

    // decimal 检查
    if old_format == Some("decimal") {
        check_decimal_compat(old_obj, new_obj, path, violations);
    }

    match old_type {
        Some("object") => {
            // 检查 properties
            if let (Some(old_props), Some(new_props)) = (
                old_obj.get("properties").and_then(|v| v.as_object()),
                new_obj.get("properties").and_then(|v| v.as_object()),
            ) {
                let old_required = get_required(old_obj);
                let new_required = get_required(new_obj);

                // 检查删除的字段
                for key in old_props.keys() {
                    if !new_props.contains_key(key) {
                        violations.push(format!("{path}.{key}: field removed"));
                    }
                }

                // 检查已有字段的变化
                for (key, old_prop) in old_props {
                    if let Some(new_prop) = new_props.get(key) {
                        let field_path = if path.is_empty() {
                            key.clone()
                        } else {
                            format!("{path}.{key}")
                        };
                        check_object_compat(old_prop, new_prop, &field_path, violations);

                        // 检查 required 变化
                        let was_required = old_required.contains(key);
                        let is_required = new_required.contains(key);
                        if !was_required && is_required {
                            violations.push(format!(
                                "{field_path}: field changed from optional to required"
                            ));
                        }
                    }
                }

                // 检查新增字段
                for key in new_props.keys() {
                    if !old_props.contains_key(key) && new_required.contains(key) {
                        let field_path = if path.is_empty() {
                            key.clone()
                        } else {
                            format!("{path}.{key}")
                        };
                        violations.push(format!(
                            "{field_path}: new field is required (must be optional)"
                        ));
                    }
                }
            }

            // 检查 additionalProperties (map value 类型)
            if let (Some(old_ap), Some(new_ap)) = (
                old_obj.get("additionalProperties"),
                new_obj.get("additionalProperties"),
            ) {
                if !old_ap.is_boolean() && !new_ap.is_boolean() {
                    let ap_path = if path.is_empty() {
                        "<map-value>".to_string()
                    } else {
                        format!("{path}.<map-value>")
                    };
                    check_object_compat(old_ap, new_ap, &ap_path, violations);
                }
            }
        }
        Some("array") => {
            // 检查 items 类型变化
            if let (Some(old_items), Some(new_items)) =
                (old_obj.get("items"), new_obj.get("items"))
            {
                let items_path = if path.is_empty() {
                    "<items>".to_string()
                } else {
                    format!("{path}.<items>")
                };
                check_object_compat(old_items, new_items, &items_path, violations);
            }
        }
        _ => {}
    }
}

/// 检查 enum 兼容性
fn check_enum_compat(old_enum: &Value, new_enum: &Value, path: &str, violations: &mut Vec<String>) {
    let old_values: Vec<&str> = old_enum
        .as_array()
        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();

    let new_values: Vec<&str> = new_enum
        .as_array()
        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();

    for old_val in &old_values {
        if !new_values.contains(old_val) {
            violations.push(format!("{path}: enum variant '{old_val}' removed"));
        }
    }
}

/// 检查 decimal 兼容性
fn check_decimal_compat(
    old: &serde_json::Map<String, Value>,
    new: &serde_json::Map<String, Value>,
    path: &str,
    violations: &mut Vec<String>,
) {
    let old_precision = old.get("x-precision").and_then(|v| v.as_u64()).unwrap_or(0);
    let new_precision = new.get("x-precision").and_then(|v| v.as_u64()).unwrap_or(0);

    if new_precision < old_precision {
        violations.push(format!(
            "{path}: decimal precision decreased from {old_precision} to {new_precision}"
        ));
    }

    let old_scale = old.get("x-scale").and_then(|v| v.as_i64()).unwrap_or(0);
    let new_scale = new.get("x-scale").and_then(|v| v.as_i64()).unwrap_or(0);

    if old_scale != new_scale {
        violations.push(format!(
            "{path}: decimal scale changed from {old_scale} to {new_scale}"
        ));
    }
}

/// 从 JSON Schema object 中提取 required 字段列表
fn get_required(obj: &serde_json::Map<String, Value>) -> Vec<String> {
    obj.get("required")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema_with_props(props: &str, required: &[&str]) -> String {
        let req_json = serde_json::to_string(required).unwrap();
        format!(
            r#"{{"type":"object","properties":{props},"required":{req_json}}}"#
        )
    }

    #[test]
    fn test_add_nullable_field_ok() {
        let old = schema_with_props(
            r#"{"id":{"type":"integer"}}"#,
            &["id"],
        );
        let new = schema_with_props(
            r#"{"id":{"type":"integer"},"name":{"type":"string"}}"#,
            &["id"],
        );
        let (ok, violations) = check(&old, &new).unwrap();
        assert!(ok, "violations: {violations:?}");
    }

    #[test]
    fn test_add_required_field_fail() {
        let old = schema_with_props(
            r#"{"id":{"type":"integer"}}"#,
            &["id"],
        );
        let new = schema_with_props(
            r#"{"id":{"type":"integer"},"name":{"type":"string"}}"#,
            &["id", "name"],
        );
        let (ok, violations) = check(&old, &new).unwrap();
        assert!(!ok);
        assert!(violations.iter().any(|v| v.contains("required")));
    }

    #[test]
    fn test_remove_field_fail() {
        let old = schema_with_props(
            r#"{"id":{"type":"integer"},"name":{"type":"string"}}"#,
            &["id"],
        );
        let new = schema_with_props(
            r#"{"id":{"type":"integer"}}"#,
            &["id"],
        );
        let (ok, violations) = check(&old, &new).unwrap();
        assert!(!ok);
        assert!(violations.iter().any(|v| v.contains("removed")));
    }

    #[test]
    fn test_change_type_fail() {
        let old = schema_with_props(
            r#"{"id":{"type":"integer"}}"#,
            &["id"],
        );
        let new = schema_with_props(
            r#"{"id":{"type":"string"}}"#,
            &["id"],
        );
        let (ok, violations) = check(&old, &new).unwrap();
        assert!(!ok);
        assert!(violations.iter().any(|v| v.contains("type changed")));
    }

    #[test]
    fn test_change_format_fail() {
        let old = schema_with_props(
            r#"{"ts":{"type":"string","format":"date-time"}}"#,
            &[],
        );
        let new = schema_with_props(
            r#"{"ts":{"type":"string","format":"date"}}"#,
            &[],
        );
        let (ok, violations) = check(&old, &new).unwrap();
        assert!(!ok);
        assert!(violations.iter().any(|v| v.contains("format changed")));
    }

    #[test]
    fn test_add_to_required_fail() {
        let old = schema_with_props(
            r#"{"id":{"type":"integer"},"name":{"type":"string"}}"#,
            &["id"],
        );
        let new = schema_with_props(
            r#"{"id":{"type":"integer"},"name":{"type":"string"}}"#,
            &["id", "name"],
        );
        let (ok, violations) = check(&old, &new).unwrap();
        assert!(!ok);
        assert!(violations.iter().any(|v| v.contains("optional to required")));
    }

    #[test]
    fn test_remove_from_required_ok() {
        let old = schema_with_props(
            r#"{"id":{"type":"integer"},"name":{"type":"string"}}"#,
            &["id", "name"],
        );
        let new = schema_with_props(
            r#"{"id":{"type":"integer"},"name":{"type":"string"}}"#,
            &["id"],
        );
        let (ok, violations) = check(&old, &new).unwrap();
        assert!(ok, "violations: {violations:?}");
    }

    #[test]
    fn test_enum_add_variant_ok() {
        let old = schema_with_props(
            r#"{"status":{"type":"string","enum":["a","b"]}}"#,
            &[],
        );
        let new = schema_with_props(
            r#"{"status":{"type":"string","enum":["a","b","c"]}}"#,
            &[],
        );
        let (ok, violations) = check(&old, &new).unwrap();
        assert!(ok, "violations: {violations:?}");
    }

    #[test]
    fn test_enum_remove_variant_fail() {
        let old = schema_with_props(
            r#"{"status":{"type":"string","enum":["a","b","c"]}}"#,
            &[],
        );
        let new = schema_with_props(
            r#"{"status":{"type":"string","enum":["a","b"]}}"#,
            &[],
        );
        let (ok, violations) = check(&old, &new).unwrap();
        assert!(!ok);
        assert!(violations.iter().any(|v| v.contains("variant") && v.contains("removed")));
    }

    #[test]
    fn test_nested_object_recursive() {
        let old = schema_with_props(
            r#"{"addr":{"type":"object","properties":{"city":{"type":"string"},"zip":{"type":"string"}}}}"#,
            &[],
        );
        // 删除嵌套字段
        let new = schema_with_props(
            r#"{"addr":{"type":"object","properties":{"city":{"type":"string"}}}}"#,
            &[],
        );
        let (ok, violations) = check(&old, &new).unwrap();
        assert!(!ok);
        assert!(violations.iter().any(|v| v.contains("zip") && v.contains("removed")));
    }

    #[test]
    fn test_array_element_type_change_fail() {
        let old = schema_with_props(
            r#"{"tags":{"type":"array","items":{"type":"string"}}}"#,
            &[],
        );
        let new = schema_with_props(
            r#"{"tags":{"type":"array","items":{"type":"integer"}}}"#,
            &[],
        );
        let (ok, violations) = check(&old, &new).unwrap();
        assert!(!ok);
        assert!(violations.iter().any(|v| v.contains("type changed")));
    }

    #[test]
    fn test_decimal_precision_increase_ok() {
        let old = schema_with_props(
            r#"{"amount":{"type":"string","format":"decimal","x-precision":10,"x-scale":2}}"#,
            &[],
        );
        let new = schema_with_props(
            r#"{"amount":{"type":"string","format":"decimal","x-precision":18,"x-scale":2}}"#,
            &[],
        );
        let (ok, violations) = check(&old, &new).unwrap();
        assert!(ok, "violations: {violations:?}");
    }

    #[test]
    fn test_decimal_precision_decrease_fail() {
        let old = schema_with_props(
            r#"{"amount":{"type":"string","format":"decimal","x-precision":18,"x-scale":2}}"#,
            &[],
        );
        let new = schema_with_props(
            r#"{"amount":{"type":"string","format":"decimal","x-precision":10,"x-scale":2}}"#,
            &[],
        );
        let (ok, violations) = check(&old, &new).unwrap();
        assert!(!ok);
        assert!(violations.iter().any(|v| v.contains("precision decreased")));
    }

    #[test]
    fn test_decimal_scale_change_fail() {
        let old = schema_with_props(
            r#"{"amount":{"type":"string","format":"decimal","x-precision":10,"x-scale":2}}"#,
            &[],
        );
        let new = schema_with_props(
            r#"{"amount":{"type":"string","format":"decimal","x-precision":10,"x-scale":4}}"#,
            &[],
        );
        let (ok, violations) = check(&old, &new).unwrap();
        assert!(!ok);
        assert!(violations.iter().any(|v| v.contains("scale changed")));
    }
}
