use crate::conn::Conn;
use crate::error::ChronicleError;
use crate::Offset;
use catalog::Catalog;
use chronicle_proto::pb_catalog::{Segment, TimelineStatus};
use chronicle_proto::pb_ext::{
    RecordEventsRequest, RecordEventsRequestItem, StatusCode,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

pub(crate) struct ReplicatorState {
    #[allow(dead_code)]
    pub timeline_id: i64,
    pub term: i64,
    pub lrs: i64,
    pub lra: i64,
    pub replication_factor: usize,
    pub needs_trunc: bool,
    pub acked: HashMap<i64, HashSet<String>>,
    pub waiters: HashMap<i64, oneshot::Sender<Result<Offset, ChronicleError>>>,
    pub senders: HashMap<String, mpsc::Sender<RecordEventsRequest>>,
}

pub(crate) struct Replicator {
    state: Arc<Mutex<ReplicatorState>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for Replicator {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl Replicator {
    pub async fn create(
        timeline_id: i64,
        replication_factor: usize,
        writable_segment: &Segment,
        conns: &HashMap<String, Conn>,
    ) -> Result<Self, ChronicleError> {
        Self::start(timeline_id, 1, 0, 0, replication_factor, false, writable_segment, conns).await
    }

    pub async fn open(
        catalog: &Catalog,
        conns: &HashMap<String, Conn>,
        timeline_name: &str,
        replication_factor: usize,
    ) -> Result<Self, ChronicleError> {
        let mut tc = catalog.get_timeline(timeline_name).await?;
        let new_term = tc.term + 1;
        tc.term = new_term;
        let tc = catalog.put_timeline(&tc, tc.version).await?;

        info!(
            timeline = timeline_name,
            timeline_id = tc.timeline_id,
            term = new_term,
            "new term: starting"
        );

        let last_segment = tc.segments.last().ok_or_else(|| {
            ChronicleError::ReconciliationFailed("timeline has no segments".into())
        })?;
        let ensemble = &last_segment.ensemble;

        let mut max_lra: i64 = 0;

        for endpoint in ensemble {
            let conn = conns.get(endpoint).ok_or_else(|| {
                ChronicleError::EnsembleUnavailable(format!("no conn for unit {}", endpoint))
            })?;

            let mut attempts: u64 = 0;
            let response = loop {
                attempts += 1;
                match conn.new_term(tc.timeline_id, new_term).await {
                    Ok(resp) => break resp,
                    Err(e) if attempts < 5 => {
                        warn!(
                            endpoint = endpoint.as_str(),
                            attempt = attempts,
                            error = %e,
                            "new term: request failed, retrying"
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
                    "new term: unit fenced"
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

        let lra = max_lra;

        let mut new_segments: Vec<Segment> = tc
            .segments
            .iter()
            .filter(|seg| seg.start_offset <= lra)
            .cloned()
            .collect();

        let writable_segment = Segment {
            id: new_segments.last().map_or(1, |s| s.id + 1),
            ensemble: ensemble.clone(),
            start_offset: lra + 1,
        };
        new_segments.push(writable_segment.clone());

        let mut updated = tc.clone();
        updated.status = TimelineStatus::Active as i32;
        updated.segments = new_segments;
        updated.lra = lra;
        catalog.put_timeline(&updated, tc.version).await?;

        info!(
            timeline = timeline_name,
            term = new_term,
            lra = lra,
            "new term: complete"
        );

        Self::start(
            tc.timeline_id, new_term, lra, lra,
            replication_factor, true,
            &writable_segment, conns,
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
        conns: &HashMap<String, Conn>,
    ) -> Result<Self, ChronicleError> {
        let state = Arc::new(Mutex::new(ReplicatorState {
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

        let mut record_senders = HashMap::new();
        let mut tasks = Vec::new();

        for endpoint in &writable_segment.ensemble {
            let conn = conns.get(endpoint).ok_or_else(|| {
                ChronicleError::EnsembleUnavailable(format!("no conn for unit {}", endpoint))
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

        Ok(Self { state, tasks })
    }

    pub fn state(&self) -> &Arc<Mutex<ReplicatorState>> {
        &self.state
    }

    pub async fn replicate(&self, items: Vec<RecordEventsRequestItem>) -> Result<(), ChronicleError> {
        let senders = {
            let s = self.state.lock().unwrap();
            s.senders.values().cloned().collect::<Vec<_>>()
        };

        if senders.is_empty() {
            return Err(ChronicleError::EnsembleUnavailable("no connected units".into()));
        }

        let request = RecordEventsRequest { items };
        for sender in &senders {
            if let Err(e) = sender.send(request.clone()).await {
                warn!(error = %e, "failed to send batch to unit");
            }
        }

        Ok(())
    }
}

async fn ack_collector(
    state: Arc<Mutex<ReplicatorState>>,
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
