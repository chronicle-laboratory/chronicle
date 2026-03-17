use crate::error::ChronicleError;
use chronicle_proto::pb_ext::{
    FenceRequest, FenceResponse, FetchEventsRequest, FetchEventsResponse, RecordEventsRequest,
    RecordEventsResponse,
    chronicle_client::ChronicleClient,
};
use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};
use tracing::info;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);
const KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct Conn {
    endpoint: String,
    client: ChronicleClient<Channel>,
}

impl Conn {
    pub fn connect(endpoint: &str) -> Result<Self, ChronicleError> {
        let channel = Self::build_channel(endpoint)?;
        let client = ChronicleClient::new(channel);
        Ok(Self {
            endpoint: endpoint.to_string(),
            client,
        })
    }

    fn build_channel(endpoint: &str) -> Result<Channel, ChronicleError> {
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

    pub async fn new_term(
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

struct ConnGroup {
    conns: Vec<Conn>,
    next: AtomicUsize,
}

pub struct ConnPool {
    entries: DashMap<String, ConnGroup>,
    conns_per_unit: usize,
}

impl ConnPool {
    pub fn new(conns_per_unit: usize) -> Self {
        Self {
            entries: DashMap::new(),
            conns_per_unit: conns_per_unit.max(1),
        }
    }

    pub fn get_or_connect(&self, endpoint: &str) -> Result<Conn, ChronicleError> {
        if let Some(entry) = self.entries.get(endpoint) {
            let idx = entry.next.fetch_add(1, Ordering::Relaxed) % entry.conns.len();
            return Ok(entry.conns[idx].clone());
        }
        let mut conns = Vec::with_capacity(self.conns_per_unit);
        for _ in 0..self.conns_per_unit {
            conns.push(Conn::connect(endpoint)?);
        }
        info!(address = %endpoint, count = self.conns_per_unit, "connected to unit");
        let entry = ConnGroup {
            conns,
            next: AtomicUsize::new(0),
        };
        let conn = entry.conns[0].clone();
        self.entries.insert(endpoint.to_string(), entry);
        Ok(conn)
    }

}
