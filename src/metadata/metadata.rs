use crate::error::unit_error::UnitError;
use crate::error::unit_error::UnitError::MetadataNodeAware;
use crate::metadata::BOOTSTRAP_PARTITION_ID;
use crate::metadata::partition::Partition;
use crate::pb_metadata::UnitMeta;
use crate::storage::storage::Storage;
use dashmap::DashMap;
use log::{error, warn};
use memberlist::bytes::Bytes;
use memberlist::delegate::{CompositeDelegate, NodeDelegate, VoidDelegate};
use memberlist::net::resolver::dns::DnsResolver;
use memberlist::net::stream_layer::tcp::Tcp;
use memberlist::net::{HostAddr, NetTransportOptions, Node};
use memberlist::proto::{MaybeResolvedAddress, Meta, NodeId, SmallVec};
use memberlist::tokio::{TokioNetTransport, TokioRuntime, TokioTcp, TokioTcpMemberlist};
use memberlist::{Memberlist, Options};
use prost::Message;
use std::net::SocketAddr;
use std::sync::Arc;

struct MetadataState {
    storage: Storage,
    partitions: DashMap<u64, Partition>,
}

impl MetadataState {
    fn has_bootstrap_partition(&self) -> bool {
        self.partitions.get(&BOOTSTRAP_PARTITION_ID).is_some()
    }
}

#[derive(Clone)]
struct MetadataDelegation {
    metadata_state: Arc<MetadataState>,
}

impl NodeDelegate for MetadataDelegation {
    async fn node_meta(&self, limit: usize) -> Meta {
        let result = Meta::try_from(
            UnitMeta {
                bootstrap_partition: self.metadata_state.has_bootstrap_partition(),
            }
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

type NodeAwareVoidDelegate = VoidDelegate<NodeId, SocketAddr>;
type NodeAwareCompositeDelete = CompositeDelegate<
    NodeId,
    SocketAddr,
    NodeAwareVoidDelegate,
    NodeAwareVoidDelegate,
    NodeAwareVoidDelegate,
    NodeAwareVoidDelegate,
    NodeAwareVoidDelegate,
    NodeAwareVoidDelegate,
>;

struct Inner {
    state: Arc<MetadataState>,
    node_aware: Memberlist<
        TokioNetTransport<NodeId, DnsResolver<TokioRuntime>, TokioTcp>,
        CompositeDelegate<
            NodeId,
            SocketAddr,
            VoidDelegate<NodeId, SocketAddr>,
            VoidDelegate<NodeId, SocketAddr>,
            VoidDelegate<NodeId, SocketAddr>,
            VoidDelegate<NodeId, SocketAddr>,
            MetadataDelegation,
            VoidDelegate<NodeId, SocketAddr>,
        >,
    >,
}

struct Metadata {
    inner: Arc<Inner>,
}

struct NodeAwareOptions {
    _self: Node<NodeId, SocketAddr>,
    peers: Vec<Node<NodeId, SocketAddr>>,
    net: NetTransportOptions<NodeId, DnsResolver<TokioRuntime>, Tcp<TokioRuntime>>,
}

impl NodeAwareOptions {}

struct MetadataOptions {
    node_aware_options: NodeAwareOptions,
    storage: Storage,
}
impl Metadata {
    async fn new(options: MetadataOptions) -> Result<Self, UnitError> {
        let metadata_state = Arc::new(MetadataState {
            storage: options.storage,
            partitions: DashMap::new(),
        });

        let node_aware_options = options.node_aware_options;
        let node_aware = join_node_aware_group(node_aware_options, &metadata_state).await?;

        let metadata = Metadata {
            inner: Arc::new(Inner {
                state: metadata_state,
                node_aware,
            }),
        };
        Ok(metadata)
    }
}

async fn join_node_aware_group(
    node_aware_options: NodeAwareOptions,
    metadata_state: &Arc<MetadataState>,
) -> Result<
    Memberlist<
        TokioNetTransport<NodeId, DnsResolver<TokioRuntime>, TokioTcp>,
        CompositeDelegate<
            NodeId,
            SocketAddr,
            VoidDelegate<NodeId, SocketAddr>,
            VoidDelegate<NodeId, SocketAddr>,
            VoidDelegate<NodeId, SocketAddr>,
            VoidDelegate<NodeId, SocketAddr>,
            MetadataDelegation,
            VoidDelegate<NodeId, SocketAddr>,
        >,
    >,
    UnitError,
> {
    let delegation = MetadataDelegation {
        metadata_state: metadata_state.clone(),
    };
    let _self_info = node_aware_options._self;
    let node_aware_net_ops = NetTransportOptions::new(_self_info.id().clone()).with_bind_addresses(
        [HostAddr::from(_self_info.address().clone())]
            .into_iter()
            .collect(),
    );
    let delegate =
        CompositeDelegate::<NodeId, SocketAddr>::default().with_node_delegate(delegation);
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
