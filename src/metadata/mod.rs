use log::warn;
use memberlist::net::{Node, NodeId};
use tokio_util::bytes::Buf;

pub mod metadata;
mod node_aware;
mod partition;
mod partition_net;
mod partition_state;
mod partition_pipeline;
mod partition_store;

const BOOTSTRAP_PARTITION_ID: u64 = 8;

#[inline]
fn decode_id(node_id: &NodeId) -> u64 {
    node_id
        .as_bytes()
        .try_get_u64()
        .expect("Unexpected node id conversion.")
}

#[inline]
fn encode_id(node_id: u64) -> NodeId {
    let b_node_id = node_id.to_be_bytes().to_vec();
    NodeId::try_from(b_node_id).expect("Unexpected node id conversion.")
}
