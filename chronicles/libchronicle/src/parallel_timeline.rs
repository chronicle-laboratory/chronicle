use crate::error::ChronicleError;
use crate::unit_client::UnitClient;
use crate::{Appendable, Offset};
use chronicle_proto::pb_catalog::TimelineCatalog;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;

pub struct ParallelTimelineOptions {
    pub replication_factor: i32,
}

struct TimelineInner {
    catalog: TimelineCatalog,
    unit_clients: Vec<UnitClient>,
    lrs: AtomicI64,
}

pub struct ParallelTimeline {
    inner: Arc<TimelineInner>,
}

impl ParallelTimeline {
    pub fn new(catalog: TimelineCatalog, unit_clients: Vec<UnitClient>) -> Self {
        let lrs = AtomicI64::new(catalog.lra);
        Self {
            inner: Arc::new(TimelineInner {
                catalog,
                unit_clients,
                lrs,
            }),
        }
    }
}

impl Appendable for ParallelTimeline {
    async fn append(&self, _payload: Vec<u8>) -> Result<Offset, ChronicleError> {
        todo!()
    }
}
