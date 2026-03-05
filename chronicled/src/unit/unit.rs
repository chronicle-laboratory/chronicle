use crate::unit::admin_service::AdminService;
use crate::actor::read_handle_group::ReadHandleGroup;
use crate::actor::write_handle_group::WriteActorGroup;
use crate::error::unit_error::UnitError;
use crate::observability::{self, ServerMetrics};
use crate::option::auto_config::{AutoConfig, SystemEnv};
use crate::option::unit_options::{ServerOptions, UnitOptions};
use crate::storage::blob::compaction::CompactionPipeline;
use crate::storage::blob::manager::SegmentManager;
use crate::storage::level_iterator::LevelIterator;
use crate::storage::index::{Storage, StorageOptions};
use crate::storage::write_cache::WriteCache;
use crate::unit::timeline_state::TimelineStateManager;
use crate::unit::unit_service::UnitService;
use crate::wal::wal::{Wal, WalOptions};
use catalog::Catalog;
use chronicle_proto::pb_catalog::{UnitRegistration, UnitStatus};
use chronicle_proto::pb_ext::Event;
use chronicle_proto::pb_admin::admin_server::AdminServer;
use chronicle_proto::pb_ext::chronicle_server::ChronicleServer;
use futures_util::StreamExt;
use prost::Message;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing::{error, info, warn};

const DEFAULT_ACTOR_NUM: usize = 4;
const DEFAULT_INFLIGHT_NUM: usize = 4096;

pub struct Unit {
    context: CancellationToken,
    external_handle: JoinHandle<()>,
    compaction_pipeline: CompactionPipeline,
    catalog: Arc<dyn Catalog>,
    address: String,
    _meter_provider: opentelemetry_sdk::metrics::SdkMeterProvider,
}

impl Unit {
    pub async fn new(
        options: UnitOptions,
        catalog: Arc<dyn Catalog>,
    ) -> Result<Self, UnitError> {
        info!("unit initializing");
        let context = CancellationToken::new();

        let env = SystemEnv::detect();
        let auto = AutoConfig::from_env_with_io(&env, options.io_mode);

        let (meter_provider, meter, prometheus_registry) = observability::init_meter_provider();
        let _metrics = Arc::new(ServerMetrics::new(&meter));

        let resolved_compaction = options.compaction.resolve(&auto);
        let resolved_index = options.index.resolve(&auto);

        let storage = Storage::new(StorageOptions {
            path: options.storage.dir.clone(),
            index: Some(resolved_index),
        })?;
        info!(path = %options.storage.dir, "storage index opened");

        let wal = Wal::new(WalOptions {
            dir: options.wal.dir.clone(),
            max_segment_size: None,
            io_mode: options.io_mode,
        })
        .await?;
        info!(dir = %options.wal.dir, "wal opened");

        let capacity = resolved_compaction.write_cache_capacity_mb * 1024 * 1024;
        let write_cache = WriteCache::new(capacity);

        let remote_store: Option<Arc<dyn crate::storage::blob::remote::RemoteStore>> =
            if let Some(ref offload_opts) = resolved_compaction.offload {
                let s3 = crate::storage::blob::remote::S3RemoteStore::new(
                    offload_opts.bucket.clone(),
                    offload_opts.prefix.clone(),
                    offload_opts.endpoint.clone(),
                    offload_opts.region.clone(),
                )
                .await;
                Some(Arc::new(s3))
            } else {
                None
            };

        let segments_dir = PathBuf::from(&options.segments.dir);
        let segment_manager = Arc::new(SegmentManager::recover_with_remote(
            segments_dir,
            options.io_mode,
            remote_store.clone(),
            64,
            storage.clone(),
        )?);
        info!(dir = %options.segments.dir, "segment manager recovered");

        info!("replaying wal into write cache");
        let mut stream = wal.read_stream().await;
        let mut replayed = 0u64;
        while let Some(result) = stream.next().await {
            match result {
                Ok(data) => {
                    if let Ok(event) = Event::decode(data.as_slice()) {
                        write_cache.put_direct(event, false);
                        replayed += 1;
                    }
                }
                Err(e) => {
                    warn!(error = ?e, "wal replay error reading record");
                    break;
                }
            }
        }
        drop(stream);
        info!(events = replayed, "wal replay complete");

        let merged_reader = LevelIterator::new(
            write_cache.clone(),
            storage.clone(),
            segment_manager.clone(),
        );

        let timeline_state = Arc::new(TimelineStateManager::new());

        let write_group = Arc::new(WriteActorGroup::new(
            DEFAULT_ACTOR_NUM,
            DEFAULT_INFLIGHT_NUM,
            wal.clone(),
            write_cache.clone(),
            timeline_state.clone(),
        ));
        let read_group = Arc::new(ReadHandleGroup::new(
            DEFAULT_ACTOR_NUM,
            DEFAULT_INFLIGHT_NUM,
            merged_reader,
        ));

        let compaction_pipeline = CompactionPipeline::spawn(
            write_cache,
            segment_manager,
            storage,
            context.clone(),
            Duration::from_millis(resolved_compaction.interval_ms),
            resolved_compaction.l1_compaction_trigger,
            resolved_compaction.l2_compaction_trigger,
            remote_store,
        );
        info!(
            interval_ms = resolved_compaction.interval_ms,
            "compaction pipeline started"
        );

        let unit_service = UnitService::new(write_group, read_group, timeline_state.clone());

        let address = format!("http://{}", options.server.bind_address);

        let external_handle = bg_start_external_service(
            options.server.clone(),
            context.clone(),
            unit_service,
            prometheus_registry,
        );

        let registration = UnitRegistration {
            address: address.clone(),
            status: UnitStatus::Writable.into(),
        };
        catalog
            .register_unit(&registration)
            .await
            .map_err(|e| UnitError::Unavailable(format!("catalog registration failed: {}", e)))?;
        info!(address = %address, "unit registered in catalog");

        Ok(Self {
            context,
            external_handle,
            compaction_pipeline,
            catalog,
            address,
            _meter_provider: meter_provider,
        })
    }

    pub async fn stop(self) {
        info!("unit shutting down");

        if let Err(err) = self.catalog.unregister_unit(&self.address).await {
            warn!(error = ?err, address = %self.address, "failed to unregister unit from catalog");
        } else {
            info!(address = %self.address, "unit unregistered from catalog");
        }

        self.context.cancel();

        self.compaction_pipeline.shutdown().await;
        if let Err(err) = self.external_handle.await {
            error!(error = ?err, "unexpected error closing external service");
        }
        info!("unit stopped");
    }
}

fn bg_start_external_service(
    options: ServerOptions,
    context: CancellationToken,
    unit_service: UnitService,
    prometheus_registry: prometheus::Registry,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let (health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<ChronicleServer<UnitService>>()
            .await;

        // Spawn Prometheus metrics HTTP endpoint on gRPC port + 1.
        let metrics_addr = std::net::SocketAddr::new(
            options.bind_address.ip(),
            options.bind_address.port() + 1,
        );
        let metrics_context = context.clone();
        tokio::spawn(async move {
            serve_prometheus(metrics_addr, prometheus_registry, metrics_context).await;
        });
        info!(addr = %metrics_addr, "prometheus metrics endpoint started");

        info!(addr = %options.bind_address, "grpc service starting");
        let serve_future = Server::builder()
            .add_service(health_service)
            .add_service(AdminServer::new(AdminService))
            .add_service(ChronicleServer::new(unit_service))
            .serve_with_shutdown(options.bind_address, context.cancelled());
        info!("unit ready");
        if let Err(err) = serve_future.await {
            error!(error = %err, "grpc service error");
        }
    })
}

async fn serve_prometheus(
    addr: std::net::SocketAddr,
    registry: prometheus::Registry,
    context: CancellationToken,
) {
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use http_body_util::Full;

    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            error!(error = %e, addr = %addr, "failed to bind prometheus endpoint");
            return;
        }
    };

    loop {
        tokio::select! {
            _ = context.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(a) => a,
                    Err(_) => continue,
                };
                let registry = registry.clone();
                tokio::spawn(async move {
                    let svc = service_fn(move |_req| {
                        let registry = registry.clone();
                        async move {
                            let encoder = prometheus::TextEncoder::new();
                            let metric_families = registry.gather();
                            let body = encoder.encode_to_string(&metric_families)
                                .unwrap_or_default();
                            Ok::<_, hyper::Error>(
                                hyper::Response::new(Full::new(hyper::body::Bytes::from(body)))
                            )
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), svc)
                        .await;
                });
            }
        }
    }
}
