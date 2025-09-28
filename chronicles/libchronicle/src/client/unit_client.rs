use crate::error::ChronicleError;
use chronicle_proto::pb_ext::{
    FenceRequest, FenceResponse, FetchEventsRequest, FetchEventsResponse, RecordEventsRequest,
    RecordEventsRequestItem, RecordEventsResponse, RecordEventsResponseItem,
    chronicle_client::ChronicleClient,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

#[derive(Clone)]
pub struct UnitClient {
    endpoint: String,
    client: ChronicleClient<Channel>,
}

impl UnitClient {
    pub async fn connect(endpoint: &str) -> Result<Self, ChronicleError> {
        let client = ChronicleClient::connect(endpoint.to_string())
            .await
            .map_err(|e| ChronicleError::Transport(format!("{}: {}", endpoint, e)))?;
        Ok(Self {
            endpoint: endpoint.to_string(),
            client,
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
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
