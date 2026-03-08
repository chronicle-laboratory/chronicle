use sha2::{Digest, Sha256};

use crate::error::Result;
use crate::types::StoredSchema;

/// 计算 schema 指纹
///
/// - JsonSchema: 规范化 JSON (sorted keys) → SHA-256
/// - AvroSchema: 规范化 JSON → SHA-256
/// - Protobuf: descriptor bytes + message_name → SHA-256
pub fn compute_fingerprint(schema: &StoredSchema) -> Result<String> {
    let mut hasher = Sha256::new();

    match schema {
        StoredSchema::JsonSchema { content } => {
            // 规范化: 解析为 serde_json::Value 再序列化（sorted keys）
            let value: serde_json::Value = serde_json::from_str(content)?;
            let canonical = canonical_json(&value);
            hasher.update(b"json:");
            hasher.update(canonical.as_bytes());
        }
        StoredSchema::AvroSchema { content } => {
            let value: serde_json::Value = serde_json::from_str(content)?;
            let canonical = canonical_json(&value);
            hasher.update(b"avro:");
            hasher.update(canonical.as_bytes());
        }
        StoredSchema::ProtobufDescriptor {
            descriptor_base64,
            message_name,
        } => {
            hasher.update(b"proto:");
            hasher.update(descriptor_base64.as_bytes());
            hasher.update(b":");
            hasher.update(message_name.as_bytes());
        }
    }

    let result = hasher.finalize();
    Ok(hex::encode(&result))
}

/// 生成规范化 JSON（key 排序）
fn canonical_json(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let entries: Vec<String> = keys
                .iter()
                .map(|k| format!("{}:{}", serde_json::to_string(k).unwrap(), canonical_json(&map[*k])))
                .collect();
            format!("{{{}}}", entries.join(","))
        }
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(canonical_json).collect();
            format!("[{}]", items.join(","))
        }
        _ => serde_json::to_string(value).unwrap(),
    }
}

/// 简易 hex 编码
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fingerprint_json_schema() {
        let schema = StoredSchema::JsonSchema {
            content: r#"{"type":"object","properties":{"id":{"type":"integer"}}}"#.to_string(),
        };
        let fp = compute_fingerprint(&schema).unwrap();
        assert_eq!(fp.len(), 64); // SHA-256 hex

        // 相同内容不同 key 顺序应该产生相同指纹
        let schema2 = StoredSchema::JsonSchema {
            content: r#"{"properties":{"id":{"type":"integer"}},"type":"object"}"#.to_string(),
        };
        let fp2 = compute_fingerprint(&schema2).unwrap();
        assert_eq!(fp, fp2);
    }

    #[test]
    fn test_fingerprint_different_schemas() {
        let s1 = StoredSchema::JsonSchema {
            content: r#"{"type":"object","properties":{"id":{"type":"integer"}}}"#.to_string(),
        };
        let s2 = StoredSchema::JsonSchema {
            content: r#"{"type":"object","properties":{"name":{"type":"string"}}}"#.to_string(),
        };
        assert_ne!(
            compute_fingerprint(&s1).unwrap(),
            compute_fingerprint(&s2).unwrap()
        );
    }

    #[test]
    fn test_fingerprint_avro() {
        let schema = StoredSchema::AvroSchema {
            content: r#"{"type":"record","name":"Test","fields":[{"name":"id","type":"long"}]}"#
                .to_string(),
        };
        let fp = compute_fingerprint(&schema).unwrap();
        assert_eq!(fp.len(), 64);
    }

    #[test]
    fn test_fingerprint_protobuf() {
        let schema = StoredSchema::ProtobufDescriptor {
            descriptor_base64: "AQID".to_string(),
            message_name: "Test".to_string(),
        };
        let fp = compute_fingerprint(&schema).unwrap();
        assert_eq!(fp.len(), 64);
    }

    #[test]
    fn test_canonical_json_sorting() {
        let v1: serde_json::Value = serde_json::from_str(r#"{"b":1,"a":2}"#).unwrap();
        let v2: serde_json::Value = serde_json::from_str(r#"{"a":2,"b":1}"#).unwrap();
        assert_eq!(canonical_json(&v1), canonical_json(&v2));
    }
}
