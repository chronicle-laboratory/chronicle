//! StoredSchema → Arrow SchemaRef 转换（客户端侧）
//!
//! 直接委托给 chronicle-lexicon 的实现。

use arrow::datatypes::SchemaRef;
use chronicle_lexicon::StoredSchema;

pub fn stored_schema_to_arrow(
    schema: &StoredSchema,
) -> std::result::Result<SchemaRef, chronicle_lexicon::error::LexiconError> {
    chronicle_lexicon::stored_schema_to_arrow(schema)
}
