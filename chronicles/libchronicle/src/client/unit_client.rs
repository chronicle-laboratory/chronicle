use crate::error::ChronicleError;
use chronicle_proto::pb_ext::{
    FenceRequest, FenceResponse, FetchEventsRequest, FetchEventsResponse, RecordEventsRequest,
    RecordEventsRequestItem, RecordEventsResponse, RecordEventsResponseItem,
    chronicle_client::ChronicleClient,
};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);
const KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct UnitClient {
    endpoint: String,
    client: ChronicleClient<Channel>,
}

impl UnitClient {
    pub async fn connect(endpoint: &str) -> Result<Self, ChronicleError> {
        let channel = Self::build_channel(endpoint).await?;
        let client = ChronicleClient::new(channel);
        Ok(Self {
            endpoint: endpoint.to_string(),
            client,
        })
    }

    /// Reconnect to the same endpoint, replacing the inner channel.
    pub async fn reconnect(&mut self) -> Result<(), ChronicleError> {
        let channel = Self::build_channel(&self.endpoint).await?;
        self.client = ChronicleClient::new(channel);
        Ok(())
    }

    async fn build_channel(endpoint: &str) -> Result<Channel, ChronicleError> {
        let channel = Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| ChronicleError::Transport(format!("{}: {}", endpoint, e)))?
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .http2_keep_alive_interval(KEEP_ALIVE_INTERVAL)
            .keep_alive_timeout(KEEP_ALIVE_TIMEOUT)
            .keep_alive_while_idle(true)
            .connect_lazy();
        Ok(channel)
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Lightweight connectivity check. Returns true if the unit responds.
    pub async fn check_alive(&self) -> bool {
        // Use a fence call with timeline_id=0, term=0 as a liveness probe.
        // Units will respond (even with a fenced error), proving they're alive.
        self.fence(0, 0).await.is_ok()
    }

    /// Unary Fence RPC.
    pub async fn fence(
        &self,
        timeline_id: i64,
        term: i64,
    ) -> Result<FenceResponse, ChronicleError> {
        let mut client = self.client.clone();
        let response = client
            .fence(FenceRequest { timeline_id, term })
            .await
            .map_err(|e| ChronicleError::Transport(e.to_string()))?;
        Ok(response.into_inner())
    }

    /// Short-lived bidirectional Record stream: sends one request batch, reads one response.
    pub async fn record(
        &self,
        items: Vec<RecordEventsRequestItem>,
    ) -> Result<Vec<RecordEventsResponseItem>, ChronicleError> {
        let mut client = self.client.clone();
        let (tx, rx) = mpsc::channel(1);
        tx.send(RecordEventsRequest { items })
            .await
            .map_err(|_| ChronicleError::Internal("failed to send request".into()))?;
        drop(tx);
        let stream = ReceiverStream::new(rx);
        let response = client
            .record(stream)
            .await
            .map_err(|e| ChronicleError::Transport(e.to_string()))?;
        let mut response_stream = response.into_inner();
        let mut all_items = Vec::new();
        while let Some(resp) = response_stream
            .message()
            .await
            .map_err(|e| ChronicleError::Transport(e.to_string()))?
        {
            all_items.extend(resp.items);
        }
        Ok(all_items)
    }

    /// Open a persistent bidirectional Record stream.
    pub async fn open_record_stream(
        &self,
        buffer: usize,
    ) -> Result<
        (
            mpsc::Sender<RecordEventsRequest>,
            tonic::Streaming<RecordEventsResponse>,
        ),
        ChronicleError,
    > {
        let mut client = self.client.clone();
        let (tx, rx) = mpsc::channel(buffer);
        let stream = ReceiverStream::new(rx);
        let response = client
            .record(stream)
            .await
            .map_err(|e| ChronicleError::Transport(e.to_string()))?;
        Ok((tx, response.into_inner()))
    }

    /// Open a persistent bidirectional Fetch stream.
    pub async fn open_fetch_stream(
        &self,
        buffer: usize,
    ) -> Result<
        (
            mpsc::Sender<FetchEventsRequest>,
            tonic::Streaming<FetchEventsResponse>,
        ),
        ChronicleError,
    > {
        let mut client = self.client.clone();
        let (tx, rx) = mpsc::channel(buffer);
        let stream = ReceiverStream::new(rx);
        let response = client
            .fetch(stream)
            .await
            .map_err(|e| ChronicleError::Transport(e.to_string()))?;
        Ok((tx, response.into_inner()))
    }
}
