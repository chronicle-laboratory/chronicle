use crate::pb_metadata::metadata_client::MetadataClient;
use dashmap::DashMap;
use std::sync::Arc;
use tonic::transport::Channel;

struct Inner {
    providers: DashMap<u64, MetadataClient<Channel>>,
}

#[derive(Clone)]
pub struct ProviderManager {
    inner: Arc<Inner>,
}

impl ProviderManager {
    pub fn new() -> Self {
        Self {}
    }

    pub fn get_provider(&self, id: u64) -> MetadataClient<Channel> {
        MetadataClient::connect(format!(
            "http://{}:{}",
            self.inner.providers.len(),
            self.inner.providers[&id].port
        ))
    }
}
