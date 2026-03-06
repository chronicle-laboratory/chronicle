use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crate::error::Result;

struct SequencedBatch {
    batch: RecordBatch,
    sequence_id: u64,
}

/// Per-topic in-memory columnar buffer backed by Arrow RecordBatches.
pub struct Memtable {
    schema: SchemaRef,
    batches: RwLock<Vec<SequencedBatch>>,
    total_size: AtomicUsize,
    oldest_entry: RwLock<Option<Instant>>,
    max_sequence_id: AtomicU64,
}

/// Frozen (immutable) buffer ready for flush to Parquet.
pub struct FrozenBuffer {
    pub topic: String,
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub min_sequence_id: u64,
    pub max_sequence_id: u64,
    pub total_rows: usize,
}

impl Memtable {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            batches: RwLock::new(Vec::new()),
            total_size: AtomicUsize::new(0),
            oldest_entry: RwLock::new(None),
            max_sequence_id: AtomicU64::new(0),
        }
    }

    /// Ingest a RecordBatch into the memtable.
    pub fn ingest(&self, batch: RecordBatch, sequence_id: u64) -> Result<()> {
        let size = batch
            .columns()
            .iter()
            .map(|c| c.get_array_memory_size())
            .sum::<usize>();

        let mut batches = self.batches.write();
        batches.push(SequencedBatch { batch, sequence_id });

        self.total_size.fetch_add(size, Ordering::Relaxed);
        self.max_sequence_id.fetch_max(sequence_id, Ordering::Relaxed);

        let mut oldest = self.oldest_entry.write();
        if oldest.is_none() {
            *oldest = Some(Instant::now());
        }

        Ok(())
    }

    /// Get a read-only snapshot of all batches (for queries).
    pub fn snapshot(&self) -> Vec<RecordBatch> {
        let batches = self.batches.read();
        batches.iter().map(|sb| sb.batch.clone()).collect()
    }

    /// Freeze: atomically swap out all data and return it as a FrozenBuffer.
    pub fn freeze(&self, topic: &str) -> Option<FrozenBuffer> {
        let mut batches = self.batches.write();
        if batches.is_empty() {
            return None;
        }

        let frozen = std::mem::take(&mut *batches);
        drop(batches);

        self.total_size.store(0, Ordering::Relaxed);
        *self.oldest_entry.write() = None;

        let min_seq = frozen.iter().map(|b| b.sequence_id).min().unwrap_or(0);
        let max_seq = frozen.iter().map(|b| b.sequence_id).max().unwrap_or(0);
        let total_rows: usize = frozen.iter().map(|b| b.batch.num_rows()).sum();
        let record_batches = frozen.into_iter().map(|sb| sb.batch).collect();

        Some(FrozenBuffer {
            topic: topic.to_string(),
            schema: self.schema.clone(),
            batches: record_batches,
            min_sequence_id: min_seq,
            max_sequence_id: max_seq,
            total_rows,
        })
    }

    /// Check whether the memtable should be flushed.
    pub fn should_flush(&self, size_threshold: usize, age_threshold: Duration) -> bool {
        if self.total_size.load(Ordering::Relaxed) >= size_threshold {
            return true;
        }
        if let Some(oldest) = *self.oldest_entry.read() {
            if oldest.elapsed() >= age_threshold {
                return true;
            }
        }
        false
    }

    pub fn active_size(&self) -> usize {
        self.total_size.load(Ordering::Relaxed)
    }

    pub fn row_count(&self) -> usize {
        self.batches.read().iter().map(|b| b.batch.num_rows()).sum()
    }

    pub fn max_sequence_id(&self) -> u64 {
        self.max_sequence_id.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]))
    }

    fn test_batch(schema: &SchemaRef, values: &[i64]) -> RecordBatch {
        let col = Arc::new(Int64Array::from(values.to_vec()));
        RecordBatch::try_new(schema.clone(), vec![col]).unwrap()
    }

    #[test]
    fn ingest_and_snapshot() {
        let schema = test_schema();
        let mt = Memtable::new(schema.clone());
        mt.ingest(test_batch(&schema, &[1, 2, 3]), 1).unwrap();
        mt.ingest(test_batch(&schema, &[4, 5]), 2).unwrap();

        let snap = mt.snapshot();
        assert_eq!(snap.len(), 2);
        assert_eq!(mt.row_count(), 5);
        assert_eq!(mt.max_sequence_id(), 2);
    }

    #[test]
    fn freeze_returns_data_and_resets() {
        let schema = test_schema();
        let mt = Memtable::new(schema.clone());
        mt.ingest(test_batch(&schema, &[1, 2]), 1).unwrap();

        let frozen = mt.freeze("test_topic").unwrap();
        assert_eq!(frozen.total_rows, 2);
        assert_eq!(frozen.topic, "test_topic");

        // After freeze, memtable is empty.
        assert!(mt.freeze("test_topic").is_none());
        assert_eq!(mt.row_count(), 0);
        assert_eq!(mt.active_size(), 0);
    }

    #[test]
    fn should_flush_by_size() {
        let schema = test_schema();
        let mt = Memtable::new(schema.clone());
        mt.ingest(test_batch(&schema, &[1]), 1).unwrap();
        // Threshold of 1 byte should trigger flush.
        assert!(mt.should_flush(1, Duration::from_secs(9999)));
    }

    #[test]
    fn should_flush_by_age() {
        let schema = test_schema();
        let mt = Memtable::new(schema.clone());
        mt.ingest(test_batch(&schema, &[1]), 1).unwrap();
        // Zero-duration age threshold should trigger immediately.
        assert!(mt.should_flush(usize::MAX, Duration::ZERO));
    }
}
