use crate::error::unit_error::UnitError;
use crate::metadata::BOOTSTRAP_PARTITION_ID;
use crate::metadata::node_aware::{NodeAware, NodeAwareOptions};
use crate::metadata::partition::Partition;
use crate::storage::storage::Storage;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::Arc;

pub struct MetadataState {
    pub storage: Storage,
    pub partitions: DashMap<u64, Partition>,
    pub members: DashMap<u64, SocketAddr>,
}

impl MetadataState {
    pub fn has_bootstrap_partition(&self) -> bool {
        self.partitions.get(&BOOTSTRAP_PARTITION_ID).is_some()
    }

    pub fn get_node_address(&self, id: u64) -> Option<SocketAddr> {
        self.members.get(&id).map(|addr| addr.value().clone())
    }
}

#[derive(Clone)]
struct Metadata {
    state: Arc<MetadataState>,
    node_aware: NodeAware,
}

struct MetadataOptions {
    node_aware_options: NodeAwareOptions,
    storage: Storage,
}
impl Metadata {
    async fn new(options: MetadataOptions) -> Result<Self, UnitError> {
        let metadata_state = Arc::new(MetadataState {
            storage: options.storage,
            partitions: DashMap::new(),
            members: DashMap::new(),
        });

        let metadata = Metadata {
            state: metadata_state.clone(),
            node_aware: NodeAware::new(options.node_aware_options, metadata_state).await?,
        };
        Ok(metadata)
    }
}
