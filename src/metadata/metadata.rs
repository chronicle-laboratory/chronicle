use crate::error::unit_error::UnitError;
use crate::metadata::node_aware::{NodeAware, NodeAwareOptions};
use crate::storage::storage::Storage;
use dashmap::DashMap;
use memberlist::net::{HostAddr, NetTransportOptions, Node, NodeId};
use std::net::SocketAddr;
use std::sync::Arc;

pub struct MetadataState {
    pub storage: Storage,
    pub members: DashMap<u64, SocketAddr>,
}

impl MetadataState {
    pub fn get_node_address(&self, id: u64) -> Option<SocketAddr> {
        self.members.get(&id).map(|addr| addr.value().clone())
    }
}

#[derive(Clone)]
pub struct Metadata {
    state: Arc<MetadataState>,
    node_aware: NodeAware,
}

pub struct MetadataOptions {
    pub _self: Node<NodeId, SocketAddr>,
    pub peers: Vec<Node<NodeId, SocketAddr>>,
    pub storage: Storage,
}
impl Metadata {
    pub async fn new(options: MetadataOptions) -> Result<Self, UnitError> {
        let metadata_state = Arc::new(MetadataState {
            storage: options.storage,
            members: DashMap::new(),
        });
        let metadata = Metadata {
            state: metadata_state.clone(),
            node_aware: NodeAware::new(
                NodeAwareOptions {
                    _self: options._self,
                    peers: options.peers,
                },
                metadata_state,
            )
            .await?,
        };

        Ok(metadata)
    }
}
