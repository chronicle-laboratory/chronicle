use crate::conn::UnitClient;
use crate::error::ChronicleError;
use crate::{Cursor, FetchedEvent};
use chronicle_proto::pb_catalog::Segment;
use chronicle_proto::pb_ext::{ChunkType, FetchEventsRequest};
use futures_util::{Stream, StreamExt};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

type EventStream = Pin<Box<dyn Stream<Item = Result<FetchedEvent, ChronicleError>> + Send>>;

const MAX_RETRIES: usize = 5;
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(500);
const MAX_POLL_INTERVAL: Duration = Duration::from_secs(5);

pub struct TimelineCursor {
    timeline_id: i64,
    segments: Vec<Segment>,
    unit_clients: HashMap<String, UnitClient>,
    position: Arc<AtomicI64>,
    stream: Option<EventStream>,
    tail: bool,
    poll_interval: Duration,
    current_backoff: Duration,
}

impl TimelineCursor {
    pub fn new(
        timeline_id: i64,
        segments: Vec<Segment>,
        unit_clients: &HashMap<String, UnitClient>,
    ) -> Self {
        let start = segments.first().map(|s| s.start_offset).unwrap_or(1);
        Self {
            timeline_id,
            segments,
            unit_clients: unit_clients.clone(),
            position: Arc::new(AtomicI64::new(start)),
            stream: None,
            tail: false,
            poll_interval: DEFAULT_POLL_INTERVAL,
            current_backoff: DEFAULT_POLL_INTERVAL,
        }
    }

    pub fn with_tail(mut self) -> Self {
        self.tail = true;
        self
    }

    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self.current_backoff = interval;
        self
    }

    fn segment_for_offset(segments: &[Segment], offset: i64) -> Result<&Segment, ChronicleError> {
        segments
            .iter()
            .rev()
            .find(|seg| seg.start_offset <= offset)
            .ok_or_else(|| {
                ChronicleError::Internal(format!("no segment covers offset {}", offset))
            })
    }

    fn pick_unit<'a>(
        unit_clients: &'a HashMap<String, UnitClient>,
        segment: &Segment,
    ) -> Result<&'a UnitClient, ChronicleError> {
        for ep in &segment.ensemble {
            if let Some(client) = unit_clients.get(ep) {
                return Ok(client);
            }
        }
        Err(ChronicleError::EnsembleUnavailable(
            "no reachable unit in segment ensemble".into(),
        ))
    }

    async fn open_stream(
        timeline_id: i64,
        segments: &[Segment],
        unit_clients: &HashMap<String, UnitClient>,
        position: &Arc<AtomicI64>,
    ) -> Result<EventStream, ChronicleError> {
        let start = position.load(Ordering::Relaxed);
        let segment = Self::segment_for_offset(segments, start)?;
        let client = Self::pick_unit(unit_clients, segment)?;

        let (tx, mut response_stream) = client.open_fetch_stream(64).await?;

        tx.send(FetchEventsRequest {
            timeline_id,
            start_offset: start,
            end_offset: i64::MAX,
        })
        .await
        .map_err(|_| ChronicleError::Transport("fetch stream closed".into()))?;

        let position = position.clone();

        let stream = async_stream::try_stream! {
            let _tx = tx;
            while let Some(response) = response_stream
                .message()
                .await
                .map_err(|e| ChronicleError::Transport(e.to_string()))?
            {
                let is_final = matches!(
                    response.r#type(),
                    ChunkType::Full | ChunkType::Last
                );
                for event in response.event {
                    let fetched = FetchedEvent {
                        offset: event.offset,
                        timestamp: event.timestamp,
                        payload: event.payload.map(|b| b.to_vec()).unwrap_or_default(),
                        key: None,
                        schema_id: if event.schema_id != 0 { Some(event.schema_id) } else { None },
                    };
                    position.store(event.offset + 1, Ordering::Relaxed);
                    yield fetched;
                }
                if is_final {
                    break;
                }
            }
        };

        Ok(Box::pin(stream))
    }
}

#[async_trait::async_trait]
impl Cursor for TimelineCursor {
    async fn fetch(&mut self) -> Result<Option<FetchedEvent>, ChronicleError> {
        let mut retries = 0;
        loop {
            if self.stream.is_none() {
                self.stream = Some(
                    Self::open_stream(
                        self.timeline_id,
                        &self.segments,
                        &self.unit_clients,
                        &self.position,
                    )
                    .await?,
                );
            }

            let stream = self.stream.as_mut().unwrap();
            match stream.next().await {
                Some(Ok(item)) => {
                    self.current_backoff = self.poll_interval;
                    return Ok(Some(item));
                }
                Some(Err(e)) => {
                    retries += 1;
                    if retries > MAX_RETRIES {
                        return Err(e);
                    }
                    warn!(
                        error = %e,
                        retry = retries,
                        "fetch stream error, reconnecting"
                    );
                    self.stream = None;
                    let backoff = Duration::from_millis(100 * (1 << retries.min(5)));
                    tokio::time::sleep(backoff).await;
                }
                None => {
                    if self.tail {
                        tokio::time::sleep(self.current_backoff).await;
                        self.current_backoff =
                            (self.current_backoff * 2).min(MAX_POLL_INTERVAL);
                        self.stream = None;
                        continue;
                    }
                    return Ok(None);
                }
            }
        }
    }

    fn seek(&mut self, offset: i64) {
        self.position.store(offset, Ordering::Relaxed);
        self.stream = None;
        self.current_backoff = self.poll_interval;
    }

    fn position(&self) -> i64 {
        self.position.load(Ordering::Relaxed)
    }
}
