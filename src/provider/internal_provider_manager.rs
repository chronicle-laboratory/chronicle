use crate::error::unit_error::UnitError;
use crate::error::unit_error::UnitError::Transport;
use crate::pb_metadata::MailPostChunk;
use crate::pb_metadata::metadata_client::MetadataClient;
use dashmap::{DashMap, Entry};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tonic::transport::Channel;

struct Inner {
    providers: DashMap<SocketAddr, MetadataClient<Channel>>,
    pending_init_providers: DashMap<SocketAddr, JoinHandle<()>>,
    // todo: multiple streams
    mail_post_streams: DashMap<SocketAddr, Sender<MailPostChunk>>,
    pending_init_mail_post_streams: DashMap<SocketAddr, JoinHandle<()>>,
}

#[derive(Clone)]
pub struct InternalProviderManager {
    inner: Arc<Inner>,
}

impl InternalProviderManager {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                providers: DashMap::new(),
                pending_init_providers: DashMap::new(),
                mail_post_streams: DashMap::new(),
                pending_init_mail_post_streams: DashMap::new(),
            }),
        }
    }

    pub fn try_get_mail_post_stream(
        &self,
        _partition_id: u64,
        target: SocketAddr,
    ) -> Option<Sender<MailPostChunk>> {
        match self.inner.mail_post_streams.entry(target) {
            Entry::Occupied(entry) => Some(entry.get().clone()),
            Entry::Vacant(entry) => None,
        }
    }

    pub fn get_provider(&self, target: SocketAddr) -> Result<MetadataClient<Channel>, UnitError> {
        match self.inner.providers.entry(target) {
            Entry::Occupied(entry) => Some(entry.get().clone()),
            Entry::Vacant(entry) => None,
        }
    }

    pub fn try_get_provider(&self, target: SocketAddr) -> Option<MetadataClient<Channel>> {
        match self.inner.providers.entry(target) {
            Entry::Occupied(entry) => Some(entry.get().clone()),
            Entry::Vacant(entry) => None,
        }
    }
}
