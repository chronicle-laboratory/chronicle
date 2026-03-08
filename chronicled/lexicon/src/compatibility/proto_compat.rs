//! Protobuf Forward Compatibility 检查
//!
//! 规则:
//! - 新增字段（新 field number）→ OK
//! - 删除已有字段（field number 消失）→ FAIL
//! - 修改已有 field number 的类型 → FAIL
//! - 修改已有 field number 的名称 → FAIL
//! - 复用已删除的 field number → FAIL
//! - 新增 enum value → OK
//! - 删除 enum value → FAIL

use prost_for_types::Message as _;

use crate::error::{LexiconError, Result};

/// 检查 Protobuf forward compatibility
///
/// descriptor 参数是 base64 编码的 FileDescriptorProto
pub fn check(
    old_desc_b64: &str,
    old_msg_name: &str,
    new_desc_b64: &str,
    new_msg_name: &str,
) -> Result<(bool, Vec<String>)> {
    use base64::Engine;

    let old_bytes = base64::engine::general_purpose::STANDARD
        .decode(old_desc_b64)
        .map_err(|e| LexiconError::InvalidSchema(format!("invalid old descriptor base64: {e}")))?;
    let new_bytes = base64::engine::general_purpose::STANDARD
        .decode(new_desc_b64)
        .map_err(|e| LexiconError::InvalidSchema(format!("invalid new descriptor base64: {e}")))?;

    let old_fd = prost_types::FileDescriptorProto::decode(old_bytes.as_slice())
        .map_err(|e| LexiconError::InvalidSchema(format!("invalid old descriptor: {e}")))?;
    let new_fd = prost_types::FileDescriptorProto::decode(new_bytes.as_slice())
        .map_err(|e| LexiconError::InvalidSchema(format!("invalid new descriptor: {e}")))?;

    let old_msg = old_fd
        .message_type
        .iter()
        .find(|m| m.name.as_deref() == Some(old_msg_name))
        .ok_or_else(|| {
            LexiconError::InvalidSchema(format!(
                "message {old_msg_name} not found in old descriptor"
            ))
        })?;

    let new_msg = new_fd
        .message_type
        .iter()
        .find(|m| m.name.as_deref() == Some(new_msg_name))
        .ok_or_else(|| {
            LexiconError::InvalidSchema(format!(
                "message {new_msg_name} not found in new descriptor"
            ))
        })?;

    let mut violations = Vec::new();
    check_message_compat(old_msg, new_msg, "", &mut violations);

    // 检查 enum 类型
    for old_enum in &old_fd.enum_type {
        let enum_name = old_enum.name.as_deref().unwrap_or("unknown");
        if let Some(new_enum) = new_fd
            .enum_type
            .iter()
            .find(|e| e.name == old_enum.name)
        {
            check_enum_compat(old_enum, new_enum, enum_name, &mut violations);
        }
    }

    Ok((violations.is_empty(), violations))
}

/// 检查 message 兼容性
fn check_message_compat(
    old_msg: &prost_types::DescriptorProto,
    new_msg: &prost_types::DescriptorProto,
    path: &str,
    violations: &mut Vec<String>,
) {
    // 建立 old field number → field 映射
    let old_fields: std::collections::HashMap<i32, &prost_types::FieldDescriptorProto> = old_msg
        .field
        .iter()
        .filter_map(|f| f.number.map(|n| (n, f)))
        .collect();

    let new_fields: std::collections::HashMap<i32, &prost_types::FieldDescriptorProto> = new_msg
        .field
        .iter()
        .filter_map(|f| f.number.map(|n| (n, f)))
        .collect();

    // 检查删除的字段
    for (&num, old_field) in &old_fields {
        let field_name = old_field.name.as_deref().unwrap_or("unknown");
        let field_path = if path.is_empty() {
            field_name.to_string()
        } else {
            format!("{path}.{field_name}")
        };

        match new_fields.get(&num) {
            None => {
                violations.push(format!(
                    "{field_path}: field number {num} removed"
                ));
            }
            Some(new_field) => {
                // 检查类型变化
                if old_field.r#type != new_field.r#type {
                    violations.push(format!(
                        "{field_path}: field number {num} type changed"
                    ));
                }
                // 检查名称变化
                if old_field.name != new_field.name {
                    violations.push(format!(
                        "{field_path}: field number {num} name changed from {:?} to {:?}",
                        old_field.name, new_field.name
                    ));
                }
            }
        }
    }

    // 检查嵌套 message
    for old_nested in &old_msg.nested_type {
        let nested_name = old_nested.name.as_deref().unwrap_or("unknown");
        if let Some(new_nested) = new_msg
            .nested_type
            .iter()
            .find(|m| m.name == old_nested.name)
        {
            let nested_path = if path.is_empty() {
                nested_name.to_string()
            } else {
                format!("{path}.{nested_name}")
            };
            check_message_compat(old_nested, new_nested, &nested_path, violations);
        }
    }

    // 检查嵌套 enum
    for old_enum in &old_msg.enum_type {
        let enum_name = old_enum.name.as_deref().unwrap_or("unknown");
        if let Some(new_enum) = new_msg
            .enum_type
            .iter()
            .find(|e| e.name == old_enum.name)
        {
            let enum_path = if path.is_empty() {
                enum_name.to_string()
            } else {
                format!("{path}.{enum_name}")
            };
            check_enum_compat(old_enum, new_enum, &enum_path, violations);
        }
    }
}

/// 检查 enum 兼容性
fn check_enum_compat(
    old_enum: &prost_types::EnumDescriptorProto,
    new_enum: &prost_types::EnumDescriptorProto,
    path: &str,
    violations: &mut Vec<String>,
) {
    for old_value in &old_enum.value {
        let value_name = old_value.name.as_deref().unwrap_or("unknown");
        let found = new_enum.value.iter().any(|v| {
            v.name == old_value.name && v.number == old_value.number
        });
        if !found {
            violations.push(format!("{path}: enum value '{value_name}' removed or renumbered"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    fn make_descriptor(msg: prost_types::DescriptorProto) -> String {
        let fd = prost_types::FileDescriptorProto {
            message_type: vec![msg],
            ..Default::default()
        };
        let bytes = fd.encode_to_vec();
        base64::engine::general_purpose::STANDARD.encode(&bytes)
    }

    fn simple_field(name: &str, number: i32, typ: i32) -> prost_types::FieldDescriptorProto {
        prost_types::FieldDescriptorProto {
            name: Some(name.to_string()),
            number: Some(number),
            r#type: Some(typ),
            label: Some(
                prost_types::field_descriptor_proto::Label::Optional as i32,
            ),
            ..Default::default()
        }
    }

    const TYPE_INT64: i32 = prost_types::field_descriptor_proto::Type::Int64 as i32;
    const TYPE_STRING: i32 = prost_types::field_descriptor_proto::Type::String as i32;
    const TYPE_BOOL: i32 = prost_types::field_descriptor_proto::Type::Bool as i32;

    #[test]
    fn test_add_field_ok() {
        let old = prost_types::DescriptorProto {
            name: Some("Test".to_string()),
            field: vec![simple_field("id", 1, TYPE_INT64)],
            ..Default::default()
        };
        let new = prost_types::DescriptorProto {
            name: Some("Test".to_string()),
            field: vec![
                simple_field("id", 1, TYPE_INT64),
                simple_field("name", 2, TYPE_STRING),
            ],
            ..Default::default()
        };

        let (ok, violations) = check(&make_descriptor(old), "Test", &make_descriptor(new), "Test").unwrap();
        assert!(ok, "violations: {violations:?}");
    }

    #[test]
    fn test_remove_field_fail() {
        let old = prost_types::DescriptorProto {
            name: Some("Test".to_string()),
            field: vec![
                simple_field("id", 1, TYPE_INT64),
                simple_field("name", 2, TYPE_STRING),
            ],
            ..Default::default()
        };
        let new = prost_types::DescriptorProto {
            name: Some("Test".to_string()),
            field: vec![simple_field("id", 1, TYPE_INT64)],
            ..Default::default()
        };

        let (ok, violations) = check(&make_descriptor(old), "Test", &make_descriptor(new), "Test").unwrap();
        assert!(!ok);
        assert!(violations.iter().any(|v| v.contains("removed")));
    }

    #[test]
    fn test_change_type_fail() {
        let old = prost_types::DescriptorProto {
            name: Some("Test".to_string()),
            field: vec![simple_field("id", 1, TYPE_INT64)],
            ..Default::default()
        };
        let new = prost_types::DescriptorProto {
            name: Some("Test".to_string()),
            field: vec![simple_field("id", 1, TYPE_STRING)],
            ..Default::default()
        };

        let (ok, violations) = check(&make_descriptor(old), "Test", &make_descriptor(new), "Test").unwrap();
        assert!(!ok);
        assert!(violations.iter().any(|v| v.contains("type changed")));
    }

    #[test]
    fn test_rename_field_fail() {
        let old = prost_types::DescriptorProto {
            name: Some("Test".to_string()),
            field: vec![simple_field("id", 1, TYPE_INT64)],
            ..Default::default()
        };
        let new = prost_types::DescriptorProto {
            name: Some("Test".to_string()),
            field: vec![simple_field("identifier", 1, TYPE_INT64)],
            ..Default::default()
        };

        let (ok, violations) = check(&make_descriptor(old), "Test", &make_descriptor(new), "Test").unwrap();
        assert!(!ok);
        assert!(violations.iter().any(|v| v.contains("name changed")));
    }

    #[test]
    fn test_enum_add_value_ok() {
        let old = prost_types::DescriptorProto {
            name: Some("Test".to_string()),
            field: vec![simple_field("status", 1, TYPE_BOOL)],
            enum_type: vec![prost_types::EnumDescriptorProto {
                name: Some("Status".to_string()),
                value: vec![
                    prost_types::EnumValueDescriptorProto {
                        name: Some("A".to_string()),
                        number: Some(0),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };
        let new = prost_types::DescriptorProto {
            name: Some("Test".to_string()),
            field: vec![simple_field("status", 1, TYPE_BOOL)],
            enum_type: vec![prost_types::EnumDescriptorProto {
                name: Some("Status".to_string()),
                value: vec![
                    prost_types::EnumValueDescriptorProto {
                        name: Some("A".to_string()),
                        number: Some(0),
                        ..Default::default()
                    },
                    prost_types::EnumValueDescriptorProto {
                        name: Some("B".to_string()),
                        number: Some(1),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };

        let (ok, violations) = check(&make_descriptor(old), "Test", &make_descriptor(new), "Test").unwrap();
        assert!(ok, "violations: {violations:?}");
    }

    #[test]
    fn test_enum_remove_value_fail() {
        let old = prost_types::DescriptorProto {
            name: Some("Test".to_string()),
            field: vec![simple_field("status", 1, TYPE_BOOL)],
            enum_type: vec![prost_types::EnumDescriptorProto {
                name: Some("Status".to_string()),
                value: vec![
                    prost_types::EnumValueDescriptorProto {
                        name: Some("A".to_string()),
                        number: Some(0),
                        ..Default::default()
                    },
                    prost_types::EnumValueDescriptorProto {
                        name: Some("B".to_string()),
                        number: Some(1),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };
        let new = prost_types::DescriptorProto {
            name: Some("Test".to_string()),
            field: vec![simple_field("status", 1, TYPE_BOOL)],
            enum_type: vec![prost_types::EnumDescriptorProto {
                name: Some("Status".to_string()),
                value: vec![
                    prost_types::EnumValueDescriptorProto {
                        name: Some("A".to_string()),
                        number: Some(0),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };

        let (ok, violations) = check(&make_descriptor(old), "Test", &make_descriptor(new), "Test").unwrap();
        assert!(!ok);
        assert!(violations.iter().any(|v| v.contains("removed")));
    }
}
