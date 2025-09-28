use crate::client::unit_client::UnitClient;
use crate::error::ChronicleError;
use crate::{Cursor, Offset};
use chronicle_proto::pb_catalog::Segment;
use chronicle_proto::pb_ext::{ChunkType, FetchEventsRequest};
use futures_util::{Stream, StreamExt};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

type EventStream = Pin<Box<dyn Stream<Item = Result<(Offset, Vec<u8>), ChronicleError>> + Send>>;

/// Queue-based streaming cursor for a timeline.
///
/// Wraps a gRPC fetch stream behind a simple pull-based API. Call `fetch()`
/// to get the next event or `seek()` to jump to a different offset.
pub struct TimelineCursor {
    timeline_id: i64,
    segments: Vec<Segment>,
    unit_clients: HashMap<String, UnitClient>,
    position: Arc<AtomicI64>,
    stream: Option<EventStream>,
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
        }
    }

    /// Current read position (next offset to be consumed).
    pub fn position(&self) -> i64 {
        self.position.load(Ordering::Relaxed)
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

    /// Open a new fetch stream starting at the given offset.
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
                    let offset = Offset {
                        timeline_id: event.timeline_id,
                        offset: event.offset,
                    };
                    let payload = event
                        .payload
                        .map(|b| b.to_vec())
                        .unwrap_or_default();
                    position.store(event.offset + 1, Ordering::Relaxed);
                    yield (offset, payload);
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
    async fn fetch(&mut self) -> Result<Option<(Offset, Vec<u8>)>, ChronicleError> {
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
            Some(Ok(item)) => Ok(Some(item)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    fn seek(&mut self, offset: i64) {
        self.position.store(offset, Ordering::Relaxed);
        self.stream = None;
    }
}
