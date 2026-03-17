use crate::conn::Conn;
use crate::error::ChronicleError;
use crate::{Event as UserEvent, Offset, Writer};
use catalog::Catalog;
use chronicle_proto::pb_catalog::{Segment, TimelineStatus};
use chronicle_proto::pb_ext::{
    Event, RecordEventsRequest, RecordEventsRequestItem, StatusCode,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

const DEFAULT_BATCH_SIZE: usize = 256;
const DEFAULT_LINGER: Duration = Duration::from_millis(5);

#[derive(Debug, Clone)]
pub(crate) struct ReconciledState {
    pub term: i64,
    pub lra: i64,
    pub lrs: i64,
    pub segments: Vec<Segment>,
    pub writable_segment: Segment,
    pub catalog_version: i64,
}

pub(crate) async fn reconcile(
    catalog: &Catalog,
    unit_clients: &HashMap<String, Conn>,
    timeline_name: &str,
) -> Result<ReconciledState, ChronicleError> {
    let mut tc = catalog.get_timeline(timeline_name).await?;
    let new_term = tc.term + 1;
    tc.term = new_term;
    let tc = catalog.put_timeline(&tc, tc.version).await?;

    info!(
        timeline = timeline_name,
        timeline_id = tc.timeline_id,
        term = new_term,
        "reconciliation: starting"
    );

    let last_segment = tc.segments.last().ok_or_else(|| {
        ChronicleError::ReconciliationFailed("timeline has no segments".into())
    })?;
    let ensemble = &last_segment.ensemble;

    let mut max_lra: i64 = 0;

    for endpoint in ensemble {
        let client = unit_clients.get(endpoint).ok_or_else(|| {
            ChronicleError::EnsembleUnavailable(format!("no client for unit {}", endpoint))
        })?;

        let mut attempts: u64 = 0;
        let response = loop {
            attempts += 1;
            match client.new_term(tc.timeline_id, new_term).await {
                Ok(resp) => break resp,
                Err(e) if attempts < 5 => {
                    warn!(
                        endpoint = endpoint.as_str(),
                        attempt = attempts,
                        error = %e,
                        "reconciliation: fence request failed, retrying"
                    );
                    tokio::time::sleep(Duration::from_millis(100 * attempts)).await;
                }
                Err(e) => {
                    return Err(ChronicleError::ReconciliationFailed(format!(
                        "failed to fence unit {}: {}",
                        endpoint, e
                    )));
                }
            }
        };

        if response.code == StatusCode::Ok as i32 {
            info!(
                endpoint = endpoint.as_str(),
                lra = response.lra,
                "reconciliation: unit fenced"
            );
            if response.lra > max_lra {
                max_lra = response.lra;
            }
        } else if response.code == StatusCode::Fenced as i32 {
            return Err(ChronicleError::Fenced {
                timeline_id: tc.timeline_id,
                term: response.term,
            });
        } else {
            return Err(ChronicleError::InvalidTerm {
                current: response.term,
                requested: new_term,
            });
        }
    }

    let new_lra = max_lra;

    let mut new_segments: Vec<Segment> = tc
        .segments
        .iter()
        .filter(|seg| seg.start_offset <= new_lra)
        .cloned()
        .collect();

    let writable_segment = Segment {
        id: new_segments.last().map_or(1, |s| s.id + 1),
        ensemble: ensemble.clone(),
        start_offset: new_lra + 1,
    };
    new_segments.push(writable_segment.clone());

    let mut updated = tc.clone();
    updated.status = TimelineStatus::Active as i32;
    updated.segments = new_segments.clone();
    updated.lra = new_lra;
    let updated = catalog.put_timeline(&updated, tc.version).await?;

    info!(
        timeline = timeline_name,
        term = new_term,
        lra = new_lra,
        segments = updated.segments.len(),
        "reconciliation: complete"
    );

    Ok(ReconciledState {
        term: new_term,
        lra: new_lra,
        lrs: new_lra,
        segments: updated.segments,
        writable_segment,
        catalog_version: updated.version,
    })
}

struct PendingEvent {
    event: UserEvent,
    tx: oneshot::Sender<Result<Offset, ChronicleError>>,
}

struct TimelineState {
    timeline_id: i64,
    term: i64,
    lrs: i64,
    lra: i64,
    replication_factor: usize,
    needs_trunc: bool,
    acked: HashMap<i64, HashSet<String>>,
    waiters: HashMap<i64, oneshot::Sender<Result<Offset, ChronicleError>>>,
    senders: HashMap<String, mpsc::Sender<RecordEventsRequest>>,
}

pub struct Timeline {
    event_tx: mpsc::Sender<PendingEvent>,
    _tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for Timeline {
    fn drop(&mut self) {
        for task in &self._tasks {
            task.abort();
        }
    }
}

impl Timeline {
    pub async fn create(
        catalog: &Catalog,
        unit_clients: &HashMap<String, Conn>,
        name: &str,
        replication_factor: usize,
    ) -> Result<Self, ChronicleError> {
        let tc = catalog.create_timeline(name).await?;

        let available: Vec<String> = unit_clients.keys().cloned().collect();
        let ensemble = select_ensemble(&available, &[], &[], replication_factor).ok_or_else(
            || {
                ChronicleError::EnsembleUnavailable(format!(
                    "need {} units, have {}",
                    replication_factor,
                    available.len()
                ))
            },
        )?;

        let first_segment = Segment {
            id: 1,
            ensemble: ensemble.clone(),
            start_offset: 1,
        };

        let mut updated = tc.clone();
        updated.status = TimelineStatus::Active as i32;
        updated.segments = vec![first_segment.clone()];
        updated.term = 1;
        let updated = catalog.put_timeline(&updated, tc.version).await?;

        info!(
            timeline = name,
            timeline_id = updated.timeline_id,
            ensemble = ?ensemble,
            "timeline created"
        );

        Self::start(
            updated.timeline_id,
            1,
            0,
            0,
            replication_factor,
            false,
            &first_segment,
            unit_clients,
        )
        .await
    }

    pub(crate) async fn open(
        reconciled: ReconciledState,
        unit_clients: &HashMap<String, Conn>,
        _name: &str,
        timeline_id: i64,
        replication_factor: usize,
    ) -> Result<Self, ChronicleError> {
        Self::start(
            timeline_id,
            reconciled.term,
            reconciled.lrs,
            reconciled.lra,
            replication_factor,
            true,
            &reconciled.writable_segment,
            unit_clients,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn start(
        timeline_id: i64,
        term: i64,
        lrs: i64,
        lra: i64,
        replication_factor: usize,
        needs_trunc: bool,
        writable_segment: &Segment,
        unit_clients: &HashMap<String, Conn>,
    ) -> Result<Self, ChronicleError> {
        let mut record_senders = HashMap::new();
        let mut tasks = Vec::new();

        let state = Arc::new(Mutex::new(TimelineState {
            timeline_id,
            term,
            lrs,
            lra,
            replication_factor,
            needs_trunc,
            acked: HashMap::new(),
            waiters: HashMap::new(),
            senders: HashMap::new(),
        }));

        for endpoint in &writable_segment.ensemble {
            let conn = unit_clients.get(endpoint).ok_or_else(|| {
                ChronicleError::EnsembleUnavailable(format!("no client for unit {}", endpoint))
            })?;

            let (tx, response_stream) = conn.open_record_stream(64).await?;
            record_senders.insert(endpoint.clone(), tx);

            let state_clone = state.clone();
            let ep = endpoint.clone();
            tasks.push(tokio::spawn(async move {
                ack_collector(state_clone, ep, response_stream).await;
            }));
        }

        {
            let mut s = state.lock().unwrap();
            s.senders = record_senders;
        }

        let (event_tx, event_rx) = mpsc::channel::<PendingEvent>(DEFAULT_BATCH_SIZE * 2);

        let state_clone = state.clone();
        tasks.push(tokio::spawn(async move {
            event_group_loop(state_clone, event_rx).await;
        }));

        Ok(Self {
            event_tx,
            _tasks: tasks,
        })
    }
}

#[async_trait::async_trait]
impl Writer for Timeline {
    async fn record(&self, event: UserEvent) -> Result<Offset, ChronicleError> {
        let (tx, rx) = oneshot::channel();
        self.event_tx
            .send(PendingEvent { event, tx })
            .await
            .map_err(|_| ChronicleError::Internal("timeline closed".into()))?;
        rx.await
            .map_err(|_| ChronicleError::Internal("ack channel dropped".into()))?
    }
}

async fn event_group_loop(
    state: Arc<Mutex<TimelineState>>,
    mut event_rx: mpsc::Receiver<PendingEvent>,
) {
    let mut batch: Vec<PendingEvent> = Vec::with_capacity(DEFAULT_BATCH_SIZE);

    loop {
        batch.clear();

        match event_rx.recv().await {
            Some(first) => batch.push(first),
            None => return,
        }

        let deadline = tokio::time::sleep(DEFAULT_LINGER);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                biased;
                event = event_rx.recv() => {
                    match event {
                        Some(e) => {
                            batch.push(e);
                            if batch.len() >= DEFAULT_BATCH_SIZE {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                _ = &mut deadline => break,
            }
        }

        if batch.is_empty() {
            continue;
        }

        let (items, senders) = {
            let mut s = state.lock().unwrap();
            let mut items = Vec::with_capacity(batch.len());

            for pending in batch.drain(..) {
                let offset = s.lrs + 1;
                s.lrs = offset;

                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;

                let proto_event = Event {
                    timeline_id: s.timeline_id,
                    term: s.term,
                    offset,
                    payload: Some(pending.event.payload.into()),
                    crc32: None,
                    timestamp: now,
                    schema_id: pending.event.schema_id.unwrap_or(0),
                };

                let item = RecordEventsRequestItem {
                    event: Some(proto_event),
                    trunc: s.needs_trunc,
                    lra: s.lra,
                };
                s.needs_trunc = false;

                s.acked.insert(offset, HashSet::new());
                s.waiters.insert(offset, pending.tx);

                items.push(item);
            }

            let ensemble = s.senders.keys().cloned().collect::<Vec<_>>();
            let senders: Vec<mpsc::Sender<RecordEventsRequest>> = ensemble
                .iter()
                .filter_map(|ep| s.senders.get(ep).cloned())
                .collect();

            (items, senders)
        };

        let request = RecordEventsRequest { items };
        for sender in &senders {
            if let Err(e) = sender.send(request.clone()).await {
                warn!(error = %e, "failed to send batch to unit");
            }
        }
    }
}

async fn ack_collector(
    state: Arc<Mutex<TimelineState>>,
    endpoint: String,
    response_stream: tonic::Streaming<chronicle_proto::pb_ext::RecordEventsResponse>,
) {
    let mut response_stream = response_stream;

    while let Ok(Some(response)) = response_stream.message().await {
        let mut s = state.lock().unwrap();

        for item in &response.items {
            if item.code == StatusCode::Ok as i32 {
                if let Some(ref event) = item.event {
                    s.acked
                        .entry(event.offset)
                        .or_default()
                        .insert(endpoint.clone());
                }
            } else {
                warn!(
                    endpoint = %endpoint,
                    code = item.code,
                    "record response error from unit"
                );
            }
        }

        let rf = s.replication_factor;
        while s.lra < s.lrs {
            let next = s.lra + 1;
            let fully_acked = s
                .acked
                .get(&next)
                .is_some_and(|set| set.len() >= rf);
            if fully_acked {
                s.lra = next;
                s.acked.remove(&next);
                if let Some(waiter) = s.waiters.remove(&next) {
                    let _ = waiter.send(Ok(Offset(next)));
                }
            } else {
                break;
            }
        }
    }

    {
        let mut s = state.lock().unwrap();
        s.senders.remove(&endpoint);
    }
    warn!(endpoint = %endpoint, "record stream ended");
}

fn select_ensemble(
    available: &[String],
    include: &[String],
    exclude: &[String],
    rf: usize,
) -> Option<Vec<String>> {
    let mut ensemble: Vec<String> = include.to_vec();

    for unit in available {
        if ensemble.len() >= rf {
            break;
        }
        if !ensemble.contains(unit) && !exclude.contains(unit) {
            ensemble.push(unit.clone());
        }
    }

    if ensemble.len() >= rf {
        Some(ensemble)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_ensemble_basic() {
        let available = vec!["a".into(), "b".into(), "c".into()];
        let result = select_ensemble(&available, &[], &[], 2).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn select_ensemble_with_include() {
        let available = vec!["a".into(), "b".into(), "c".into()];
        let include = vec!["b".into()];
        let result = select_ensemble(&available, &include, &[], 2).unwrap();
        assert!(result.contains(&"b".to_string()));
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn select_ensemble_with_exclude() {
        let available = vec!["a".into(), "b".into(), "c".into()];
        let exclude = vec!["a".into(), "b".into()];
        let result = select_ensemble(&available, &[], &exclude, 2);
        assert!(result.is_none());
    }

    #[test]
    fn select_ensemble_insufficient() {
        let available = vec!["a".into()];
        assert!(select_ensemble(&available, &[], &[], 2).is_none());
    }
}
