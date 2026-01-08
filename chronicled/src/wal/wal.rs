use crate::error::unit_error::UnitError;
use crate::wal::segment::Segment;
use crate::wal::INVALID_OFFSET;
use log::info;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::select;
use tokio::sync::mpsc::{channel, Receiver as MpscReceiver};
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::sync::watch::Receiver;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::{sync, task};
use tokio_util::sync::CancellationToken;

struct Inner {
    buffer: sync::mpsc::Sender<(Vec<u8>, oneshot::Sender<i64>)>,
    synced_offset: Receiver<i64>,
    writable_segment: Mutex<Segment>,
}

impl Inner {
    async fn sync_data(&self) {
        if let Err(e) = self.writable_segment.lock().await.sync().await {
            info!("Failed to sync writable segment: {:?}", e);
        }
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
}

impl Wal {
    pub async fn new(options: WalOptions) -> Result<Wal, UnitError> {
        let (buf_tx, buf_rx) = channel::<(Vec<u8>, oneshot::Sender<i64>)>(1024);

        let (advanced_offset_tx, advanced_offset_rx) = watch::channel(INVALID_OFFSET);
        let (synced_offset_tx, synced_offset_rx) = watch::channel(INVALID_OFFSET);

        let mut path = PathBuf::from(options.dir);
        path.push("00000000.log");

        let segment = Segment::new(path)
            .await
            .map_err(|e| UnitError::Storage(e.to_string()))?;

        let context = CancellationToken::new();
        let inner = Arc::new(Inner {
            buffer: buf_tx,
            synced_offset: synced_offset_rx,
            writable_segment: Mutex::new(segment),
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
}

async fn bg_wal_writer(
    context: CancellationToken,
    inner: Arc<Inner>,
    mut buf_rx: MpscReceiver<(Vec<u8>, oneshot::Sender<i64>)>,
    advanced_offset_tx: watch::Sender<i64>,
) {
    loop {
        select! {
            _ = context.cancelled() => {
                info!("bg_wal_writer exit");
                break;
            }
            Some((data, offset_tx)) = buf_rx.recv() => {
                let mut segment = inner.writable_segment.lock().await;
                match segment.write(&data).await {
                    Ok(offset) => {
                        let offset = offset as i64;
                        if offset_tx.send(offset).is_err() {
                            info!("Failed to send offset back to caller");
                        }
                        if advanced_offset_tx.send(offset).is_err() {
                            info!("No active subscriber for advanced offset");
                        }
                    }
                    Err(e) => {
                        info!("Failed to write to segment: {:?}", e);
                    }
                }
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
