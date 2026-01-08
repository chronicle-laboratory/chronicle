use crate::banner::print_banner;
use crate::cm::unit_options::{ServerOptions, UnitOptions};
use crate::error::unit_error::UnitError;
use crate::storage::storage::{Storage, StorageOptions};
use crate::unit::unit_service::UnitService;
use crate::wal::wal::{Wal, WalOptions};
use chronicle_catalog::catalog::Catalog;
use chronicle_catalog::{CatalogOptions, new_catalog};
use chronicle_proto::pb_ext::chronicle_server::ChronicleServer;
use log::{error};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing::info;

pub struct Unit {
    context: CancellationToken,
    external_handle: JoinHandle<()>,
    prometheus_handle: JoinHandle<()>,

    catalog: Arc<Box<dyn Catalog>>,
    storage: Storage,
    write_ahead_log: Wal,
}

impl Unit {
    pub async fn new(options: UnitOptions) -> Result<Self, UnitError> {
        print_banner();
        info!("starting unit.");
        let context = CancellationToken::new();

        info!("loading the storage...");
        let storage_dir = options.storage.dir.clone();
        let storage = Storage::new(StorageOptions { path: storage_dir })?;

        info!("loading the write ahead log...");
        let wal_dir = options.wal.dir.clone();
        let write_ahead_log = Wal::new(WalOptions { dir: wal_dir }).await?;

        info!("starting the catalog...");
        let catalog = Arc::new(
            new_catalog(CatalogOptions {
                provider: options.catalog.provider,
                address: options.catalog.address,
            })
            .await?,
        );

        let server_options = options.server.clone();

        let unit_service = UnitService::new(storage.clone(), write_ahead_log.clone(), catalog.clone());
        let external_handle =
            bg_start_external_service(server_options, context.clone(), unit_service);
        let prometheus_handle = bg_start_prometheus_service();

        Ok(Self {
            context,
            storage,
            write_ahead_log,
            catalog,
            external_handle,
            prometheus_handle,
        })
    }

    pub async fn stop(self) {
        info!("closing unit.");
        self.context.cancel();

        let (public_res, prom_res) = tokio::join!(self.external_handle, self.prometheus_handle);
        if let Err(err) = public_res {
            error!("unexpected error when closing public task: {:?}", err);
        }
        if let Err(err) = prom_res {
            error!("unexpected error when closing prometheus task: {:?}", err);
        }
        info!("closed unit")
    }
}

fn bg_start_external_service(
    options: ServerOptions,
    context: CancellationToken,
    unit_service: UnitService,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("starting background public service");
        let serve_future = Server::builder()
            .add_service(ChronicleServer::new(unit_service))
            .serve_with_shutdown(options.bind_address, context.cancelled());
        info!("unit is ready to accept traffic");
        if let Err(err) = serve_future.await {
            error!("unit start external service error: {}", err)
        }
    })
}

fn bg_start_prometheus_service() -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("starting background prometheus service");
    })
}
