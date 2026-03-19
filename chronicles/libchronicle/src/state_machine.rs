use crate::conn::{Conn, ConnPool};
use crate::ensemble::EnsembleSelector;
use crate::error::ChronicleError;
use crate::Offset;
use catalog::Catalog;
use chronicle_proto::pb_catalog::Segment;
use chronicle_proto::pb_ext::{RecordEventsRequest, StatusCode};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

pub(crate) struct SharedState {
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

pub(crate) struct StateMachine {
    shared: Arc<Mutex<SharedState>>,
    pub segments: Vec<Segment>,
    ack_tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for StateMachine {
    fn drop(&mut self) {
        for task in &self.ack_tasks {
            task.abort();
        }
    }
}

impl StateMachine {
    pub async fn open(
        catalog: &Catalog,
        pool: &ConnPool,
        name: &str,
        replication_factor: usize,
        _schema_id: Option<String>,
    ) -> Result<Self, ChronicleError> {
        let tc = match catalog.get_timeline(name).await {
            Ok(tc) => tc,
            Err(catalog::error::CatalogError::NotFound(_)) => {
                catalog.create_timeline(name).await?
            }
            Err(e) => return Err(e.into()),
        };

        // Select ensemble: prefer previous ensemble members, then zone-diverse low-pressure units
        let last_segment = catalog.get_last_segment(&tc.name).await?;
        let previous: Vec<String> = last_segment
            .as_ref()
            .map(|vs| vs.value.ensemble.clone())
            .unwrap_or_default();

        let registrations = catalog.list_units().await?;
        let selector = EnsembleSelector::from_units(registrations.clone());
        let ensemble = selector
            .select(replication_factor, &previous, &[])
            .ok_or_else(|| {
                ChronicleError::EnsembleUnavailable(format!(
                    "need {} writable units, have {}",
                    replication_factor,
                    registrations
                        .iter()
                        .filter(|r| r.status()
                            == chronicle_proto::pb_catalog::UnitStatus::Writable)
                        .count()
                ))
            })?;

        let new_term = tc.term + 1;
        let mut tc_update = tc.clone();
        tc_update.term = new_term;
        let tc = catalog.put_timeline(&tc_update, tc.version).await?;

        info!(
            timeline_id = tc.timeline_id,
            term = new_term,
            "new term: fencing ensemble"
        );

        let mut conns: HashMap<String, Conn> = HashMap::new();
        for endpoint in &ensemble {
            conns.insert(
                endpoint.clone(),
                pool.get_or_connect(endpoint)?,
            );
        }

        let mut max_lra: i64 = 0;

        for (endpoint, conn) in &conns {
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

        let existing_segments = catalog.list_segments(&tc.name).await?;

        let existing_version = existing_segments
            .iter()
            .find(|vs| vs.value.start_offset == lra + 1)
            .map(|vs| vs.version)
            .unwrap_or(-1);

        let mut segments: Vec<Segment> = existing_segments
            .into_iter()
            .filter(|vs| vs.value.start_offset <= lra)
            .map(|vs| vs.value)
            .collect();

        let writable_segment = Segment {
            ensemble: ensemble.clone(),
            start_offset: lra + 1,
        };
        let writable_segment = catalog
            .put_segment(&tc.name, &writable_segment, existing_version)
            .await?;
        segments.push(writable_segment.value);

        let mut updated = tc.clone();
        updated.lra = lra;
        let updated = catalog.put_timeline(&updated, tc.version).await?;

        info!(
            timeline_id = tc.timeline_id,
            term = new_term,
            lra = lra,
            "new term: complete"
        );

        // Build shared state
        let shared = Arc::new(Mutex::new(SharedState {
            timeline_id: tc.timeline_id,
            term: new_term,
            lrs: lra,
            lra,
            replication_factor,
            needs_trunc: lra > 0,
            acked: HashMap::new(),
            waiters: HashMap::new(),
            senders: HashMap::new(),
        }));

        // Open record streams and start ack collectors
        let mut record_senders = HashMap::new();
        let mut ack_tasks = Vec::new();

        for (endpoint, conn) in &conns {
            let (tx, response_stream) = conn.open_record_stream(64).await?;
            record_senders.insert(endpoint.clone(), tx);

            let shared_clone = shared.clone();
            let ep = endpoint.clone();
            ack_tasks.push(tokio::spawn(async move {
                ack_collector(shared_clone, ep, response_stream).await;
            }));
        }

        {
            let mut s = shared.lock().unwrap();
            s.senders = record_senders;
        }

        let _catalog_version = updated.version;

        Ok(Self {
            shared,
            segments,
            ack_tasks,
        })
    }

    pub fn shared(&self) -> Arc<Mutex<SharedState>> {
        self.shared.clone()
    }

    pub fn timeline_id(&self) -> i64 {
        self.shared.lock().unwrap().timeline_id
    }

    pub fn lra(&self) -> i64 {
        self.shared.lock().unwrap().lra
    }

    pub async fn close(&self) {
        let mut s = self.shared.lock().unwrap();
        s.senders.clear();
    }
}

// --- ack collector ---

async fn ack_collector(
    shared: Arc<Mutex<SharedState>>,
    endpoint: String,
    response_stream: tonic::Streaming<chronicle_proto::pb_ext::RecordEventsResponse>,
) {
    let mut response_stream = response_stream;

    while let Ok(Some(response)) = response_stream.message().await {
        let mut s = shared.lock().unwrap();

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
        let mut s = shared.lock().unwrap();
        s.senders.remove(&endpoint);
    }
    warn!(endpoint = %endpoint, "record stream ended");
}
