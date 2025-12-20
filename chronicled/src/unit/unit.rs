use crate::banner::print_banner;
use crate::cm::unit_options::UnitOptions;
use crate::error::unit_error::UnitError;
use crate::error::unit_error::UnitError::TaskError;
use crate::metadata::encode_id;
use crate::metadata::metadata::{Metadata, MetadataOptions};
use crate::storage::storage::{Storage, StorageOptions};
use crate::wal::wal::{Wal, WalOptions};
use log::error;
use memberlist::net::Node;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::info;

pub struct Handles {
    public: Option<JoinHandle<()>>,
    prometheus: Option<JoinHandle<()>>,
}

pub struct Unit {
    metadata: Metadata,
    handles: Mutex<Handles>,
}

impl Unit {
    pub async fn new(options: UnitOptions) -> Result<Self, UnitError> {
        print_banner();

        info!("starting unit.");

        info!("loading the storage...");
        let storage_dir = options.storage.dir.clone();
        let storage = Storage::new(StorageOptions { path: storage_dir })?;

        info!("loading the write ahead log...");
        let wal_dir = options.wal.dir.clone();
        let wal = Wal::new(WalOptions { dir: wal_dir })?;

        info!("starting the metadata store...");
        let unit_id = encode_id(options.meta.id);
        let meta_bind_address = options.meta.bind_address;
        let node = Node::new(unit_id, meta_bind_address);

        let metadata_store = Metadata::new(MetadataOptions {
            _self: node,
            peers: vec![],
            storage,
        })
        .await?;

        let external_handle = bg_start_external_service();
        let prometheus_handle = bg_start_prometheus_service();

        Ok(Self {
            metadata: metadata_store,
            handles: Mutex::new(Handles {
                public: Some(external_handle),
                prometheus: Some(prometheus_handle),
            }),
        })
    }

    pub async fn stop(self) {
        info!("closing unit.");

        let mut guard = self.handles.lock().await;
        if let Some(public_handle) = guard.public.take() {
            public_handle.abort();

            match public_handle.await {
                Ok(__) => {}
                Err(err) => {
                    error!("unexpected error when closing public task: {}", err);
                }
            }
        }
        if let Some(prometheus_handle) = guard.prometheus.take() {
            prometheus_handle.abort();

            match prometheus_handle.await {
                Ok(__) => {}
                Err(err) => {
                    error!("unexpected error when closing prometheus task: {}", err);
                }
            }
        }

        self.metadata.stop().await;

        info!("closed unit")
    }
}

fn bg_start_external_service() -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("starting background public service");

        info!("unit is ready to accept traffic")
    })
}
fn bg_start_prometheus_service() -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("starting background prometheus service");
    })
}
