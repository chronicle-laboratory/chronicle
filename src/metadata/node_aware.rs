use crate::error::unit_error::UnitError;
use crate::error::unit_error::UnitError::MetadataNodeAware;
use crate::metadata::decode_id;
use crate::metadata::metadata::MetadataState;
use crate::pb_metadata::UnitMeta;
use hyper::body::Bytes;
use log::error;
use memberlist::delegate::{CompositeDelegate, EventDelegate, NodeDelegate, VoidDelegate};
use memberlist::net::resolver::dns::DnsResolver;
use memberlist::net::stream_layer::tcp::Tcp;
use memberlist::net::{HostAddr, NetTransportOptions, Node, NodeId, TokioNetTransport};
use memberlist::proto::{MaybeResolvedAddress, Meta, NodeState};
use memberlist::tokio::{TokioRuntime, TokioTcp, TokioTcpMemberlist};
use memberlist::{Memberlist, Options};
use prost::Message;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Clone)]
struct MetadataDelegation {
    metadata_state: Arc<MetadataState>,
}

impl NodeDelegate for MetadataDelegation {
    async fn node_meta(&self, limit: usize) -> Meta {
        let result = Meta::try_from(
            UnitMeta { }
            .encode_to_vec(),
        );
        if result.is_err() {
            error!("Node meta exceeds the limit");
            return Meta::default();
        }
        result.unwrap()
    }
    async fn local_state(&self, join: bool) -> Bytes {
        Bytes::new()
    }

    async fn merge_remote_state(&self, buf: &[u8], join: bool) -> () {}
}

impl EventDelegate for MetadataDelegation {
    type Id = NodeId;
    type Address = SocketAddr;

    async fn notify_join(&self, node: Arc<NodeState<Self::Id, Self::Address>>) -> () {
        let id = decode_id(&node.id);
        self.metadata_state.members.insert(id, node.addr.clone());
        ()
    }

    async fn notify_leave(&self, node: Arc<NodeState<Self::Id, Self::Address>>) -> () {
        let id = decode_id(&node.id);
        self.metadata_state.members.remove(&id);
        ()
    }

    async fn notify_update(&self, node: Arc<NodeState<Self::Id, Self::Address>>) -> () {
        let id = decode_id(&node.id);
        self.metadata_state.members.insert(id, node.addr.clone());
        ()
    }
}

type NodeAwareMemberlist = Memberlist<
    TokioNetTransport<NodeId, DnsResolver<TokioRuntime>, TokioTcp>,
    CompositeDelegate<
        NodeId,
        SocketAddr,
        VoidDelegate<NodeId, SocketAddr>,
        VoidDelegate<NodeId, SocketAddr>,
        MetadataDelegation,
        VoidDelegate<NodeId, SocketAddr>,
        MetadataDelegation,
        VoidDelegate<NodeId, SocketAddr>,
    >,
>;

struct Inner {
    node_aware: NodeAwareMemberlist,
}
pub struct NodeAwareOptions {
    pub _self: Node<NodeId, SocketAddr>,
    pub peers: Vec<Node<NodeId, SocketAddr>>,
}

#[derive(Clone)]
pub struct NodeAware {
    inner: Arc<Inner>,
    metadata_state: Arc<MetadataState>,
}

impl NodeAware {
    pub async fn new(
        options: NodeAwareOptions,
        metadata_state: Arc<MetadataState>,
    ) -> Result<Self, UnitError> {
        let node_aware = join_node_aware_group(options, &metadata_state).await?;
        Ok(Self {
            inner: Arc::new(Inner { node_aware }),
            metadata_state,
        })
    }

    pub fn try_get_node_address(&self, node_id: u64) -> Option<SocketAddr> {
        self.metadata_state.get_node_address(node_id)
    }

    pub fn get_node_address(&self, node_id: u64) -> Result<SocketAddr, UnitError> {
        match self.metadata_state.get_node_address(node_id) {
            None => Err(MetadataNodeAware(format!(
                "Node address not found. node: {}",
                node_id
            ))),
            Some(node_addr) => Ok(node_addr),
        }
    }
}

async fn join_node_aware_group(
    node_aware_options: NodeAwareOptions,
    metadata_state: &Arc<MetadataState>,
) -> Result<NodeAwareMemberlist, UnitError> {
    let delegation = MetadataDelegation {
        metadata_state: metadata_state.clone(),
    };
    let _self_info = node_aware_options._self;
    let node_aware_net_ops = NetTransportOptions::new(_self_info.id().clone()).with_bind_addresses(
        [HostAddr::from(_self_info.address().clone())]
            .into_iter()
            .collect(),
    );
    let delegate = CompositeDelegate::<NodeId, SocketAddr>::default()
        .with_node_delegate(delegation.clone())
        .with_event_delegate(delegation);
    let option = Options::wan();
    let node_aware = TokioTcpMemberlist::with_delegate(delegate, node_aware_net_ops, option)
        .await
        .map_err(|err| MetadataNodeAware(format!("{:?}", err)))?;

    let peers_iter = node_aware_options.peers.into_iter().map(|x| {
        Node::new(
            x.id().clone(),
            MaybeResolvedAddress::Resolved(x.address().clone()),
        )
    });
    if let Err(err) = node_aware.join_many(peers_iter).await {
        let joined = err.0.len();
        if joined == 0 {
            return Err(MetadataNodeAware(format!(
                "unable to join the existing group. err: {:?}",
                err.1
            )));
        }
    };
    Ok(node_aware)
}
