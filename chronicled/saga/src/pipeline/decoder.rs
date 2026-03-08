use crate::error::{Result, SagaError};
use arrow::array::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int32Builder, Int64Builder, RecordBatch, StringBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use std::sync::Arc;

/// Column builder enum to avoid trait object limitations.
enum ColumnBuilder {
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
    TimestampMs(TimestampMillisecondBuilder),
    TimestampUs(TimestampMicrosecondBuilder),
    Date32(Date32Builder),
    Boolean(BooleanBuilder),
    Binary(BinaryBuilder),
}

impl ColumnBuilder {
    fn finish(&mut self) -> arrow::array::ArrayRef {
        match self {
            Self::Int32(b) => Arc::new(b.finish()),
            Self::Int64(b) => Arc::new(b.finish()),
            Self::Float32(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::Utf8(b) => Arc::new(b.finish()),
            Self::TimestampMs(b) => Arc::new(b.finish()),
            Self::TimestampUs(b) => Arc::new(b.finish()),
            Self::Date32(b) => Arc::new(b.finish()),
            Self::Boolean(b) => Arc::new(b.finish()),
            Self::Binary(b) => Arc::new(b.finish()),
        }
    }

    fn append_null(&mut self) {
        match self {
            Self::Int32(b) => b.append_null(),
            Self::Int64(b) => b.append_null(),
            Self::Float32(b) => b.append_null(),
            Self::Float64(b) => b.append_null(),
            Self::Utf8(b) => b.append_null(),
            Self::TimestampMs(b) => b.append_null(),
            Self::TimestampUs(b) => b.append_null(),
            Self::Date32(b) => b.append_null(),
            Self::Boolean(b) => b.append_null(),
            Self::Binary(b) => b.append_null(),
        }
    }
}

/// Decode protobuf Rows into an Arrow RecordBatch using the given Arrow schema.
///
/// The schema is typically fetched from Lexicon (schema registry).
pub fn decode_rows(
    rows: &[chronicle_proto::pb_saga::Row],
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    let mut builders = create_builders(schema, rows.len());

    for row in rows {
        if row.fields.len() != schema.fields().len() {
            return Err(SagaError::Decode(format!(
                "row has {} fields, schema expects {}",
                row.fields.len(),
                schema.fields().len()
            )));
        }

        for (i, field) in schema.fields().iter().enumerate() {
            let fv = &row.fields[i];
            append_value(&mut builders[i], field.data_type(), fv)?;
        }
    }

    let columns: Vec<arrow::array::ArrayRef> = builders.iter_mut().map(|b| b.finish()).collect();

    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

fn create_builders(schema: &SchemaRef, capacity: usize) -> Vec<ColumnBuilder> {
    schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Int32 => ColumnBuilder::Int32(Int32Builder::with_capacity(capacity)),
            DataType::Int64 => ColumnBuilder::Int64(Int64Builder::with_capacity(capacity)),
            DataType::Float32 => ColumnBuilder::Float32(Float32Builder::with_capacity(capacity)),
            DataType::Float64 => ColumnBuilder::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Utf8 => {
                ColumnBuilder::Utf8(StringBuilder::with_capacity(capacity, capacity * 32))
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                ColumnBuilder::TimestampMs(TimestampMillisecondBuilder::with_capacity(capacity))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                ColumnBuilder::TimestampUs(TimestampMicrosecondBuilder::with_capacity(capacity))
            }
            DataType::Date32 => ColumnBuilder::Date32(Date32Builder::with_capacity(capacity)),
            DataType::Boolean => ColumnBuilder::Boolean(BooleanBuilder::with_capacity(capacity)),
            DataType::Binary => {
                ColumnBuilder::Binary(BinaryBuilder::with_capacity(capacity, capacity * 64))
            }
            // Fallback: treat unknown types as Utf8 (JSON string representation)
            _ => ColumnBuilder::Utf8(StringBuilder::with_capacity(capacity, capacity * 32)),
        })
        .collect()
}

fn append_value(
    builder: &mut ColumnBuilder,
    data_type: &DataType,
    fv: &chronicle_proto::pb_saga::FieldValue,
) -> Result<()> {
    use chronicle_proto::pb_saga::field_value::Value;

    if fv.is_null {
        builder.append_null();
        return Ok(());
    }

    match (data_type, builder, &fv.value) {
        (DataType::Int32, ColumnBuilder::Int32(b), Some(Value::IntValue(v))) => {
            b.append_value(*v as i32);
        }
        (DataType::Int64, ColumnBuilder::Int64(b), Some(Value::IntValue(v))) => {
            b.append_value(*v);
        }
        (DataType::Float32, ColumnBuilder::Float32(b), Some(Value::FloatValue(v))) => {
            b.append_value(*v as f32);
        }
        (DataType::Float64, ColumnBuilder::Float64(b), Some(Value::FloatValue(v))) => {
            b.append_value(*v);
        }
        (DataType::Utf8, ColumnBuilder::Utf8(b), Some(Value::StringValue(v))) => {
            b.append_value(v);
        }
        (
            DataType::Timestamp(TimeUnit::Millisecond, _),
            ColumnBuilder::TimestampMs(b),
            Some(Value::TimestampMs(v)),
        ) => {
            b.append_value(*v);
        }
        (
            DataType::Timestamp(TimeUnit::Microsecond, _),
            ColumnBuilder::TimestampUs(b),
            Some(Value::TimestampMs(v)),
        ) => {
            // Convert ms to us
            b.append_value(*v * 1000);
        }
        (DataType::Date32, ColumnBuilder::Date32(b), Some(Value::IntValue(v))) => {
            b.append_value(*v as i32);
        }
        (DataType::Boolean, ColumnBuilder::Boolean(b), Some(Value::BoolValue(v))) => {
            b.append_value(*v);
        }
        (DataType::Binary, ColumnBuilder::Binary(b), Some(Value::BytesValue(v))) => {
            b.append_value(v);
        }
        // Fallback: coerce to Utf8 string representation
        (_, ColumnBuilder::Utf8(b), Some(value)) => {
            let s = match value {
                Value::IntValue(v) => v.to_string(),
                Value::FloatValue(v) => v.to_string(),
                Value::StringValue(v) => v.clone(),
                Value::TimestampMs(v) => v.to_string(),
                Value::BoolValue(v) => v.to_string(),
                Value::BytesValue(v) => base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    v,
                ),
            };
            b.append_value(&s);
        }
        (_, builder, None) => {
            builder.append_null();
        }
        _ => {
            return Err(SagaError::Decode(format!(
                "type mismatch: expected {:?}",
                data_type,
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use chronicle_proto::pb_saga::field_value::Value;
    use chronicle_proto::pb_saga::{FieldValue, Row};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    #[test]
    fn decode_basic_rows() {
        let rows = vec![
            Row {
                fields: vec![
                    FieldValue {
                        value: Some(Value::TimestampMs(1000)),
                        is_null: false,
                    },
                    FieldValue {
                        value: Some(Value::IntValue(42)),
                        is_null: false,
                    },
                ],
            },
            Row {
                fields: vec![
                    FieldValue {
                        value: Some(Value::TimestampMs(2000)),
                        is_null: false,
                    },
                    FieldValue {
                        value: None,
                        is_null: true,
                    },
                ],
            },
        ];

        let schema = test_schema();
        let batch = decode_rows(&rows, &schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn decode_empty_rows() {
        let schema = test_schema();
        let batch = decode_rows(&[], &schema).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn decode_field_count_mismatch() {
        let rows = vec![Row {
            fields: vec![FieldValue {
                value: Some(Value::IntValue(1)),
                is_null: false,
            }],
        }];
        let schema = test_schema();
        assert!(decode_rows(&rows, &schema).is_err());
    }

    #[test]
    fn decode_int32_and_float32() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Float32, false),
        ]));
        let rows = vec![Row {
            fields: vec![
                FieldValue {
                    value: Some(Value::IntValue(42)),
                    is_null: false,
                },
                FieldValue {
                    value: Some(Value::FloatValue(3.14)),
                    is_null: false,
                },
            ],
        }];
        let batch = decode_rows(&rows, &schema).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }
}
