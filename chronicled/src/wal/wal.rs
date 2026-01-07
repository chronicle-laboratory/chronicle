use crate::error::unit_error::UnitError;
use crate::wal::INVALID_OFFSET;
use crate::wal::segment::Segment;
use log::info;
use std::sync::{Arc, Mutex};
use sync::mpsc::channel;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch;
use tokio::sync::watch::Receiver;
use tokio::task::JoinHandle;
use tokio::{sync, task};
use tokio_util::sync::CancellationToken;

struct Inner {
    buffer: Sender<Vec<u8>>,
    synced_offset: Receiver<i64>,
    writable_segment: Segment,
}

impl Inner {
    async fn sync_data(&self) {}
}

#[derive(Clone)]
pub struct Wal {
    context: CancellationToken,
    inner: Arc<Inner>,
    wal_writer_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    wal_syncer_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

pub struct WalOptions {
    pub dir: String,
}

impl Wal {
    pub fn new(options: WalOptions) -> Result<Wal, UnitError> {
        let (buf_tx, buf_rx) = channel(1024);

        let (advanced_offset_tx, advanced_offset_rx) = watch::channel(INVALID_OFFSET);
        let (synced_offset_tx, synced_offset_rx) = watch::channel(INVALID_OFFSET);

        let context = CancellationToken::new();
        let inner = Arc::new(Inner {
            buffer: buf_tx,
            synced_offset: synced_offset_rx,
        });

        let wal_writer_handle = task::spawn(bg_wal_writer(advanced_offset_tx));
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

    pub async fn append(&self, data: Vec<u8>) -> Result<i64, UnitError> {}

    pub fn watch_synced(&self) -> Receiver<i64> {
        self.inner.synced_offset.clone()
    }
}

async fn bg_wal_writer(advanced_offset_tx: watch::Sender<i64>) {
    loop {}
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
