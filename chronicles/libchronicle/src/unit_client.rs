use crate::error::ChronicleError;
use chronicle_proto::pb_ext::chronicle_client::ChronicleClient;
use chronicle_proto::pb_ext::{FenceRequest, FenceResponse};
use dashmap::DashMap;
use tonic::transport::Channel;

#[derive(Clone)]
pub struct UnitClient {
    client: ChronicleClient<Channel>,
}

impl UnitClient {
    pub async fn connect(address: &str) -> Result<Self, ChronicleError> {
        let endpoint = if address.starts_with("http") {
            address.to_string()
        } else {
            format!("http://{}", address)
        };

        let client = ChronicleClient::connect(endpoint)
            .await
            .map_err(|e| ChronicleError::Transport(e.to_string()))?;

        Ok(Self { client })
    }

    pub async fn fence(
        &mut self,
        timeline_id: i64,
        term: i64,
    ) -> Result<FenceResponse, ChronicleError> {
        let request = FenceRequest { timeline_id, term };

        self.client
            .fence(request)
            .await
            .map(|r| r.into_inner())
            .map_err(|e| ChronicleError::Transport(e.to_string()))
    }
}

pub struct UnitClientPool {
    clients: DashMap<String, UnitClient>,
}

impl UnitClientPool {
    pub fn new() -> Self {
        Self {
            clients: DashMap::new(),
        }
    }

    pub async fn get_or_connect(&self, address: &str) -> Result<UnitClient, ChronicleError> {
        if let Some(client) = self.clients.get(address) {
            return Ok(client.clone());
        }
        let client = UnitClient::connect(address).await?;
        self.clients.insert(address.to_string(), client.clone());
        Ok(client)
    }
}
