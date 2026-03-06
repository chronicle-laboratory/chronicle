use crate::error::{Result, SagaError};
use crate::types::TopicConfig;
use arrow::array::{
    BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch,
    StringBuilder, TimestampMillisecondBuilder,
};
use arrow::datatypes::{DataType, TimeUnit};
use std::sync::Arc;

/// Column builder enum to avoid trait object limitations.
enum ColumnBuilder {
    Int64(Int64Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
    TimestampMs(TimestampMillisecondBuilder),
    Boolean(BooleanBuilder),
    Binary(BinaryBuilder),
}

impl ColumnBuilder {
    fn finish(&mut self) -> arrow::array::ArrayRef {
        match self {
            Self::Int64(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::Utf8(b) => Arc::new(b.finish()),
            Self::TimestampMs(b) => Arc::new(b.finish()),
            Self::Boolean(b) => Arc::new(b.finish()),
            Self::Binary(b) => Arc::new(b.finish()),
        }
    }

    fn append_null(&mut self) {
        match self {
            Self::Int64(b) => b.append_null(),
            Self::Float64(b) => b.append_null(),
            Self::Utf8(b) => b.append_null(),
            Self::TimestampMs(b) => b.append_null(),
            Self::Boolean(b) => b.append_null(),
            Self::Binary(b) => b.append_null(),
        }
    }
}

/// Decode protobuf Rows into an Arrow RecordBatch using the topic schema.
pub fn decode_rows(
    rows: &[chronicle_proto::pb_saga::Row],
    topic_config: &TopicConfig,
) -> Result<RecordBatch> {
    let schema = topic_config.to_arrow_schema();

    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let mut builders = create_builders(&schema, rows.len());

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

    Ok(RecordBatch::try_new(schema, columns)?)
}

fn create_builders(
    schema: &arrow::datatypes::SchemaRef,
    capacity: usize,
) -> Vec<ColumnBuilder> {
    schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Int64 => ColumnBuilder::Int64(Int64Builder::with_capacity(capacity)),
            DataType::Float64 => {
                ColumnBuilder::Float64(Float64Builder::with_capacity(capacity))
            }
            DataType::Utf8 => {
                ColumnBuilder::Utf8(StringBuilder::with_capacity(capacity, capacity * 32))
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                ColumnBuilder::TimestampMs(TimestampMillisecondBuilder::with_capacity(capacity))
            }
            DataType::Boolean => {
                ColumnBuilder::Boolean(BooleanBuilder::with_capacity(capacity))
            }
            DataType::Binary => {
                ColumnBuilder::Binary(BinaryBuilder::with_capacity(capacity, capacity * 64))
            }
            _ => ColumnBuilder::Int64(Int64Builder::with_capacity(capacity)),
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
        (DataType::Int64, ColumnBuilder::Int64(b), Some(Value::IntValue(v))) => {
            b.append_value(*v);
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
        (DataType::Boolean, ColumnBuilder::Boolean(b), Some(Value::BoolValue(v))) => {
            b.append_value(*v);
        }
        (DataType::Binary, ColumnBuilder::Binary(b), Some(Value::BytesValue(v))) => {
            b.append_value(v);
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
    use crate::types::{FieldDef, FieldType, PartitionGranularity};
    use chronicle_proto::pb_saga::field_value::Value;
    use chronicle_proto::pb_saga::{FieldValue, Row};

    fn test_topic() -> TopicConfig {
        TopicConfig {
            name: "test".into(),
            schema: vec![
                FieldDef {
                    name: "timestamp".into(),
                    data_type: FieldType::TimestampMillis,
                    nullable: false,
                },
                FieldDef {
                    name: "value".into(),
                    data_type: FieldType::Int64,
                    nullable: true,
                },
            ],
            sort_keys: vec!["timestamp".into()],
            partition_granularity: PartitionGranularity::None,
        }
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

        let batch = decode_rows(&rows, &test_topic()).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn decode_empty_rows() {
        let batch = decode_rows(&[], &test_topic()).unwrap();
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
        assert!(decode_rows(&rows, &test_topic()).is_err());
    }
}
