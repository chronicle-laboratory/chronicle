use memberlist::net::NodeId;
use tokio_util::bytes::Buf;

pub mod metadata;
mod node_aware;

#[inline]
pub fn decode_id(node_id: &NodeId) -> u64 {
    node_id
        .as_bytes()
        .try_get_u64()
        .expect("Unexpected node id conversion.")
}

#[inline]
pub fn encode_id(node_id: u64) -> NodeId {
    let b_node_id = node_id.to_be_bytes().to_vec();
    NodeId::try_from(b_node_id).expect("Unexpected node id conversion.")
}
