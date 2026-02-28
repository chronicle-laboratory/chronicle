use crate::error::unit_error::UnitError;
use crate::wal::record::{Record, RecordBatch};
use crate::wal::segment::Segment;
use crate::wal::INVALID_OFFSET;
use async_stream::stream;
use futures_util::stream::Stream;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::{channel, Receiver as MpscReceiver};
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::sync::watch::Receiver;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::interval;
use tokio::{sync, task};
use tokio_util::sync::CancellationToken;
use tracing::info;

const DEFAULT_MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;
const BATCH_FLUSH_INTERVAL_MS: u64 = 10;
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
}

pub struct WalOptions {
    pub dir: String,
    pub max_segment_size: Option<u64>,
    pub recycle: bool,
}

#[derive(Clone)]
pub struct Wal {
    context: CancellationToken,
    inner: Arc<Inner>,
    wal_writer_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    wal_syncer_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl Wal {
    pub async fn new(options: WalOptions) -> Result<Wal, UnitError> {
        let (buf_tx, buf_rx) = channel::<(Vec<u8>, oneshot::Sender<i64>)>(1024);

        let (advanced_offset_tx, advanced_offset_rx) = watch::channel(INVALID_OFFSET);
        let (synced_offset_tx, synced_offset_rx) = watch::channel(INVALID_OFFSET);

        let mut path = PathBuf::from(&options.dir);

        if let Err(e) = tokio::fs::create_dir_all(&options.dir).await {
            return Err(UnitError::Storage(format!(
                "Failed to create WAL directory: {}",
                e
            )));
        }

        path.push("00000000.log");

        let segment = if options.recycle {
            Segment::new_with_recycle(path)
                .await
                .map_err(|e| UnitError::Storage(e.to_string()))?
        } else {
            Segment::new(path)
                .await
                .map_err(|e| UnitError::Storage(e.to_string()))?
        };

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

    pub async fn append(&self, data: Vec<u8>) -> Result<i64, UnitError> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .buffer
            .send((data, tx))
            .await
            .map_err(|_| UnitError::Wal)?;
        rx.await.map_err(|_| UnitError::Wal)
    }

    pub fn watch_synced(&self) -> Receiver<i64> {
        self.inner.synced_offset.clone()
    }

    pub async fn read_stream(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<u8>, UnitError>> + Send + '_>> {
        let mut segment = self.inner.writable_segment.lock().await;
        let data_result = segment.read_all().await;
        drop(segment);

        let data = match data_result {
            Ok(d) => d,
            Err(e) => {
                return Box::pin(stream! {
                    yield Err(UnitError::Storage(e.to_string()));
                });
            }
        };

        Box::pin(stream! {
            let mut offset = 0;

            while offset < data.len() {
                match Record::decode(&data[offset..]) {
                    Ok((record, size)) => {
                        yield Ok(record.data);
                        offset += size;
                    }
                    Err(e) => {
                        info!("Failed to decode record at offset {}: {}", offset, e);
                        break;
                    }
                }
            }
        })
    }

    pub fn cancel(&self) {
        self.context.cancel();
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
                if !pending_batch.is_empty() {
                    flush_batch(&inner, pending_batch, pending_senders, &advanced_offset_tx).await;
                }
                info!("bg_wal_writer exit");
                break;
            }
            _ = batch_timer.tick() => {
                if !pending_batch.is_empty() {
                    let batch = std::mem::replace(&mut pending_batch, RecordBatch::new());
                    let senders = std::mem::take(&mut pending_senders);
                    flush_batch(&inner, batch, senders, &advanced_offset_tx).await;
                }
            }
            Some((data, offset_tx)) = buf_rx.recv() => {
                let record = Record::new(data);
                pending_batch.add(record);
                pending_senders.push(offset_tx);

                if pending_batch.len() >= MAX_BATCH_SIZE {
                    let batch = std::mem::replace(&mut pending_batch, RecordBatch::new());
                    let senders = std::mem::take(&mut pending_senders);
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
    let record_sizes: Result<Vec<usize>, _> = batch
        .records
        .iter()
        .map(|r| r.encode().map(|e| e.len()))
        .collect();

    let record_sizes = match record_sizes {
        Ok(sizes) => sizes,
        Err(e) => {
            info!("Failed to encode records in batch: {:?}", e);
            return;
        }
    };

    let encoded = match batch.encode() {
        Ok(e) => e,
        Err(e) => {
            info!("Failed to encode batch: {:?}", e);
            return;
        }
    };

    let mut segment = inner.writable_segment.lock().await;

    match segment.write(&encoded).await {
        Ok(base_offset) => {
            let base_offset = base_offset as i64;

            let mut current_offset = base_offset;
            for (sender, size) in senders.into_iter().zip(record_sizes.iter()) {
                if sender.send(current_offset).is_err() {
                    info!("Failed to send offset back to caller");
                }
                current_offset += *size as i64;
            }

            let final_offset = base_offset + encoded.len() as i64;
            if advanced_offset_tx.send(final_offset).is_err() {
                info!("No active subscriber for advanced offset");
            }
        }
        Err(e) => {
            info!("Failed to write batch to segment: {:?}", e);
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
                info!("Watch advanced offset channel failed. error={:?}", err);
                break;
            }
        };
    }
}
