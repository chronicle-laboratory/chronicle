use crate::error::unit_error::UnitError;
use crate::wal::record::{Record, RecordBatch};
use crate::wal::segment::Segment;
use crate::wal::INVALID_OFFSET;
use log::info;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::{channel, Receiver as MpscReceiver};
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::sync::watch::Receiver;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{interval, Interval};
use tokio::{sync, task};
use tokio_util::sync::CancellationToken;

// Maximum segment size before rotation (64MB)
const DEFAULT_MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

// Batch flush interval
const BATCH_FLUSH_INTERVAL_MS: u64 = 10;

// Maximum batch size
const MAX_BATCH_SIZE: usize = 512;

struct Inner {
    buffer: sync::mpsc::Sender<(Vec<u8>, oneshot::Sender<i64>)>,
    synced_offset: Receiver<i64>,
    writable_segment: Mutex<Segment>,
    max_segment_size: u64,
}

impl Inner {
    async fn sync_data(&self) {
        if let Err(e) = self.writable_segment.lock().await.sync().await {
            info!("Failed to sync writable segment: {:?}", e);
        }
    }

    async fn should_rotate(&self) -> bool {
        let segment = self.writable_segment.lock().await;
        segment.size() >= self.max_segment_size
    }
}

#[derive(Clone)]
pub struct Wal {
    context: CancellationToken,
    inner: Arc<Inner>,
    wal_writer_handle: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
    wal_syncer_handle: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
}

pub struct WalOptions {
    pub dir: String,
    pub max_segment_size: Option<u64>,
}

impl Wal {
    pub async fn new(options: WalOptions) -> Result<Wal, UnitError> {
        let (buf_tx, buf_rx) = channel::<(Vec<u8>, oneshot::Sender<i64>)>(1024);

        let (advanced_offset_tx, advanced_offset_rx) = watch::channel(INVALID_OFFSET);
        let (synced_offset_tx, synced_offset_rx) = watch::channel(INVALID_OFFSET);

        let mut path = PathBuf::from(&options.dir);
        
        // Create directory if it doesn't exist
        if let Err(e) = tokio::fs::create_dir_all(&options.dir).await {
            return Err(UnitError::Storage(format!("Failed to create WAL directory: {}", e)));
        }
        
        path.push("00000000.log");

        let segment = Segment::new(path)
            .await
            .map_err(|e| UnitError::Storage(e.to_string()))?;

        let context = CancellationToken::new();
        let max_segment_size = options.max_segment_size.unwrap_or(DEFAULT_MAX_SEGMENT_SIZE);
        
        let inner = Arc::new(Inner {
            buffer: buf_tx,
            synced_offset: synced_offset_rx,
            writable_segment: Mutex::new(segment),
            max_segment_size,
        });

        let wal_writer_handle = task::spawn(bg_wal_writer(
            context.clone(),
            inner.clone(),
            buf_rx,
            advanced_offset_tx,
        ));
        let wal_syncer_handle = task::spawn(bg_wal_syncer(
            inner.clone(),
            advanced_offset_rx,
            synced_offset_tx,
        ));

        Ok(Wal {
            context,
            inner,
            wal_writer_handle: Arc::new(Mutex::new(Some(wal_writer_handle))),
            wal_syncer_handle: Arc::new(Mutex::new(Some(wal_syncer_handle))),
        })
    }

    pub fn close() -> Result<(), UnitError> {
        Ok(())
    }

    /// Append a single record to the WAL
    pub async fn append(&self, data: Vec<u8>) -> Result<i64, UnitError> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .buffer
            .send((data, tx))
            .await
            .map_err(|_| UnitError::Wal)?;
        rx.await.map_err(|_| UnitError::Wal)
    }

    /// Append multiple records as a batch
    pub async fn append_batch(&self, batch: Vec<Vec<u8>>) -> Result<Vec<i64>, UnitError> {
        let mut offsets = Vec::with_capacity(batch.len());
        for data in batch {
            let offset = self.append(data).await?;
            offsets.push(offset);
        }
        Ok(offsets)
    }

    pub fn watch_synced(&self) -> Receiver<i64> {
        self.inner.synced_offset.clone()
    }

    /// Read all records from the WAL (for recovery)
    pub async fn read_all(&self) -> Result<Vec<Vec<u8>>, UnitError> {
        let mut segment = self.inner.writable_segment.lock().await;
        let data = segment.read_all().await
            .map_err(|e| UnitError::Storage(e.to_string()))?;
        
        let mut records = Vec::new();
        let mut offset = 0;
        
        while offset < data.len() {
            match Record::decode(&data[offset..]) {
                Ok((record, size)) => {
                    records.push(record.data);
                    offset += size;
                }
                Err(e) => {
                    info!("Failed to decode record at offset {}: {}", offset, e);
                    break;
                }
            }
        }
        
        Ok(records)
    }
}

async fn bg_wal_writer(
    context: CancellationToken,
    inner: Arc<Inner>,
    mut buf_rx: MpscReceiver<(Vec<u8>, oneshot::Sender<i64>)>,
    advanced_offset_tx: watch::Sender<i64>,
) {
    let mut batch_timer = interval(Duration::from_millis(BATCH_FLUSH_INTERVAL_MS));
    batch_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    
    let mut pending_batch = RecordBatch::new();
    let mut pending_senders = Vec::new();
    
    loop {
        select! {
            _ = context.cancelled() => {
                // Flush remaining batch before exit
                if !pending_batch.is_empty() {
                    flush_batch(&inner, pending_batch, pending_senders, &advanced_offset_tx).await;
                }
                info!("bg_wal_writer exit");
                break;
            }
            _ = batch_timer.tick() => {
                if !pending_batch.is_empty() {
                    let batch = std::mem::replace(&mut pending_batch, RecordBatch::new());
                    let senders = std::mem::replace(&mut pending_senders, Vec::new());
                    flush_batch(&inner, batch, senders, &advanced_offset_tx).await;
                }
            }
            Some((data, offset_tx)) = buf_rx.recv() => {
                let record = Record::new(data);
                pending_batch.add(record);
                pending_senders.push(offset_tx);
                
                // Flush if batch is full
                if pending_batch.len() >= MAX_BATCH_SIZE {
                    let batch = std::mem::replace(&mut pending_batch, RecordBatch::new());
                    let senders = std::mem::replace(&mut pending_senders, Vec::new());
                    flush_batch(&inner, batch, senders, &advanced_offset_tx).await;
                }
            }
        }
    }
}

async fn flush_batch(
    inner: &Arc<Inner>,
    batch: RecordBatch,
    senders: Vec<oneshot::Sender<i64>>,
    advanced_offset_tx: &watch::Sender<i64>,
) {
    let encoded = batch.encode();
    let mut segment = inner.writable_segment.lock().await;
    
    match segment.write(&encoded).await {
        Ok(base_offset) => {
            let base_offset = base_offset as i64;
            
            // Send offset to each sender (for simplicity, use base_offset for all)
            // In a real implementation, you might want to track individual offsets
            for sender in senders {
                if sender.send(base_offset).is_err() {
                    info!("Failed to send offset back to caller");
                }
            }
            
            // Notify about the latest offset
            if advanced_offset_tx.send(base_offset).is_err() {
                info!("No active subscriber for advanced offset");
            }
        }
        Err(e) => {
            info!("Failed to write batch to segment: {:?}", e);
            // Notify callers of the error
            for sender in senders {
                let _ = sender.send(INVALID_OFFSET);
            }
        }
    }
}

async fn bg_wal_syncer(
    inner: Arc<Inner>,
    mut advanced_offset_rx: watch::Receiver<i64>,
    synced_offset_tx: watch::Sender<i64>,
) {
    loop {
        match advanced_offset_rx.changed().await {
            Ok(_) => {
                let advanced_offset = *advanced_offset_rx.borrow();
                inner.sync_data().await;
                if let Err(err) = synced_offset_tx.send(advanced_offset) {
                    info!(
                        "No active subscriber watch the synced offset. error={:?}",
                        err
                    )
                }
            }
            Err(err) => {
                info!("Watch advanced offset channel failed. error={:?}", err)
            }
        };
    }
}
