use crate::banner::print_banner;
use crate::cm::unit_options::UnitOptions;
use crate::error::unit_error::UnitError;
use crate::metadata::encode_id;
use crate::metadata::metadata::{Metadata, MetadataOptions};
use crate::storage::storage::{Storage, StorageOptions};
use crate::wal::wal::{Wal, WalOptions};
use memberlist::net::Node;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;
use tracing::info;

pub struct Handles {
    public: JoinHandle<()>,
    prometheus: JoinHandle<()>,
}

#[derive(Clone)]
pub struct Unit {
    metadata: Arc<Metadata>,
    handles: Arc<Mutex<Handles>>,
}

impl Unit {
    pub async fn new(options: UnitOptions) -> Result<Self, UnitError> {
        print_banner();

        info!("starting unit.");

        info!("loading the storage");
        let storage_dir = options.storage.dir.clone();
        let storage = Storage::new(StorageOptions { path: storage_dir })?;

        info!("loading the write ahead log");
        let wal_dir = options.wal.dir.clone();
        let wal = Wal::new(WalOptions { dir: wal_dir })?;

        info!("starting the metadata store");
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
            metadata: Arc::new(metadata_store),
            handles: Arc::new(Mutex::new(Handles {
                public: external_handle,
                prometheus: prometheus_handle,
            })),
        })
    }
}

fn bg_start_external_service() -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("starting background external service");
    })
}
fn bg_start_prometheus_service() -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("starting background prometheus service");
    })
}
