//! 兼容性检查统一入口
//!
//! 按 wire_format 分发到对应的兼容性检查器。

pub mod avro_compat;
pub mod json_compat;
pub mod proto_compat;

use crate::error::{LexiconError, Result};
use crate::types::{StoredSchema, WireFormat};

/// 按 wire_format 分发到对应的兼容性检查器
///
/// 返回 (compatible, violations)
pub fn check_compatibility(
    wire_format: &WireFormat,
    old_schema: &StoredSchema,
    new_schema: &StoredSchema,
) -> Result<(bool, Vec<String>)> {
    match (wire_format, old_schema, new_schema) {
        (
            WireFormat::Json,
            StoredSchema::JsonSchema { content: old },
            StoredSchema::JsonSchema { content: new },
        ) => json_compat::check(old, new),

        (
            WireFormat::Avro,
            StoredSchema::AvroSchema { content: old },
            StoredSchema::AvroSchema { content: new },
        ) => avro_compat::check(old, new),

        (
            WireFormat::Protobuf,
            StoredSchema::ProtobufDescriptor {
                descriptor_base64: old_desc,
                message_name: old_msg,
            },
            StoredSchema::ProtobufDescriptor {
                descriptor_base64: new_desc,
                message_name: new_msg,
            },
        ) => proto_compat::check(old_desc, old_msg, new_desc, new_msg),

        _ => Err(LexiconError::WireFormatMismatch {
            expected: format!("{wire_format:?}"),
            got: "mismatched schema types".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_mismatch() {
        let old = StoredSchema::JsonSchema {
            content: "{}".to_string(),
        };
        let new = StoredSchema::AvroSchema {
            content: "{}".to_string(),
        };
        let result = check_compatibility(&WireFormat::Json, &old, &new);
        assert!(result.is_err());
    }
}
