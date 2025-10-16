use crate::banner::print_banner;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;
use tracing::info;

pub struct UnitOptions {}

struct Inner {}

pub struct Handles {
    internal: JoinHandle<()>,
    external: JoinHandle<()>,
    prometheus: JoinHandle<()>,
}

pub struct Unit {
    inner: Arc<Inner>,
    handles: Arc<Mutex<Handles>>,
}

impl Unit {
    pub fn new(options: UnitOptions) -> Self {
        let inner = Arc::new(Inner {});
        print_banner();

        let internal_handle = bg_start_internal_service();
        let external_handle = bg_start_external_service();
        let prometheus_handle = bg_start_prometheus_service();
        Self {
            inner,
            handles: Arc::new(Mutex::new(Handles {
                internal: internal_handle,
                external: external_handle,
                prometheus: prometheus_handle,
            })),
        }
    }
}
fn bg_start_external_service() -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("starting background external service");
    })
}
fn bg_start_internal_service() -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("starting background internal service");
    })
}
fn bg_start_prometheus_service() -> JoinHandle<()> {
    tokio::spawn(async move {
        info!("starting background prometheus service");
    })
}
