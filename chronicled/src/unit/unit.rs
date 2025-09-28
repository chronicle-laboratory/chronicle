use crate::actor::read_handle_group::ReadHandleGroup;
use crate::actor::write_handle_group::WriteActorGroup;
use crate::option::unit_options::{ServerOptions, UnitOptions};
use crate::error::unit_error::UnitError;
use crate::storage::storage::{Storage, StorageOptions};
use crate::unit::timeline_state::TimelineStateManager;
use crate::unit::unit_service::UnitService;
use crate::wal::wal::{Wal, WalOptions};
use chronicle_proto::pb_ext::chronicle_server::ChronicleServer;
use chronicle_proto::pb_ext::Event;
use futures_util::StreamExt;
use prost::Message;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing::{error, info, warn};

const DEFAULT_ACTOR_NUM: usize = 4;
const DEFAULT_INFLIGHT_NUM: usize = 4096;

pub struct Unit {
    context: CancellationToken,
    external_handle: JoinHandle<()>,
}

impl Unit {
    pub async fn new(options: UnitOptions) -> Result<Self, UnitError> {
        info!("starting unit");
        let context = CancellationToken::new();

        // Storage
        info!("loading storage...");
        let storage = Storage::new(StorageOptions {
            path: options.storage.dir.clone(),
        })?;

        // WAL
        info!("loading WAL...");
        let wal = Wal::new(WalOptions {
            dir: options.wal.dir.clone(),
            max_segment_size: None,
            recycle: false,
        })
        .await?;

        // WAL replay into storage
        info!("replaying WAL into storage...");
        let write_cache = storage.fetch_write_cache();
        let mut stream = wal.read_stream().await;
        let mut replayed = 0u64;
        while let Some(result) = stream.next().await {
            match result {
                Ok(data) => {
                    if let Ok(event) = Event::decode(data.as_slice()) {
                        if let Err(e) = write_cache.put_with_trunc(event, false) {
                            warn!("WAL replay: failed to write event to storage: {:?}", e);
                        }
                        replayed += 1;
                    }
                }
                Err(e) => {
                    warn!("WAL replay: error reading record: {:?}", e);
                    break;
                }
            }
        }
        drop(stream);
        info!("WAL replay complete: {} events replayed", replayed);

        // Timeline state
        let timeline_state = Arc::new(TimelineStateManager::new());

        // Actor groups
        let write_group = Arc::new(WriteActorGroup::new(
            DEFAULT_ACTOR_NUM,
            DEFAULT_INFLIGHT_NUM,
            wal.clone(),
            storage.clone(),
            timeline_state.clone(),
        ));
        let read_group = Arc::new(ReadHandleGroup::new(
            DEFAULT_ACTOR_NUM,
            DEFAULT_INFLIGHT_NUM,
            storage.clone(),
        ));

        // gRPC service
        let unit_service = UnitService::new(
            write_group,
            read_group,
            timeline_state.clone(),
        );

        let external_handle = bg_start_external_service(
            options.server.clone(),
            context.clone(),
            unit_service,
        );

        Ok(Self {
            context,
            external_handle,
        })
    }

    pub async fn stop(self) {
        info!("closing unit");
        self.context.cancel();

        if let Err(err) = self.external_handle.await {
            error!("unexpected error when closing external service: {:?}", err);
        }
        info!("unit closed");
    }
}

fn bg_start_external_service(
    options: ServerOptions,
    context: CancellationToken,
    unit_service: UnitService,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("starting gRPC service on {}", options.bind_address);
        let serve_future = Server::builder()
            .add_service(ChronicleServer::new(unit_service))
            .serve_with_shutdown(options.bind_address, context.cancelled());
        info!("unit ready to accept traffic");
        if let Err(err) = serve_future.await {
            error!("unit external service error: {}", err)
        }
    })
}

