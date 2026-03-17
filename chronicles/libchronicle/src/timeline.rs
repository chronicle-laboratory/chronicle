use crate::conn::UnitClient;
use crate::error::ChronicleError;
use crate::{Event as UserEvent, RecordResult, Writer};
use catalog::Catalog;
use chronicle_proto::pb_catalog::{Segment, TimelineStatus};
use chronicle_proto::pb_ext::{
    Event, RecordEventsRequest, RecordEventsRequestItem, StatusCode,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{info, warn};

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
    catalog: &dyn Catalog,
    unit_clients: &HashMap<String, UnitClient>,
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
            match client.fence(tc.timeline_id, new_term).await {
                Ok(resp) => break resp,
                Err(e) if attempts < 5 => {
                    warn!(
                        endpoint = endpoint.as_str(),
                        attempt = attempts,
                        error = %e,
                        "reconciliation: fence request failed, retrying"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(100 * attempts)).await;
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

struct InnerState {
    timeline_id: i64,
    #[allow(dead_code)]
    name: String,
    term: i64,
    #[allow(dead_code)]
    segments: Vec<Segment>,
    writable_segment: Option<Segment>,
    lrs: i64,
    lra: i64,
    acked: HashMap<i64, HashSet<String>>,
    #[allow(dead_code)]
    catalog_version: i64,
    replication_factor: usize,
    needs_trunc: bool,
    senders: HashMap<String, mpsc::Sender<RecordEventsRequest>>,
}

pub struct Timeline {
    timeline_id: i64,
    inner: Arc<Mutex<InnerState>>,
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
        catalog: &dyn Catalog,
        unit_clients: &HashMap<String, UnitClient>,
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

        Self::open_inner(
            updated.timeline_id,
            name,
            1,
            updated.segments.clone(),
            first_segment,
            0,
            0,
            updated.version,
            replication_factor,
            false,
            unit_clients,
        )
        .await
    }

    pub(crate) async fn open(
        reconciled: ReconciledState,
        unit_clients: &HashMap<String, UnitClient>,
        name: &str,
        timeline_id: i64,
        replication_factor: usize,
    ) -> Result<Self, ChronicleError> {
        Self::open_inner(
            timeline_id,
            name,
            reconciled.term,
            reconciled.segments,
            reconciled.writable_segment,
            reconciled.lrs,
            reconciled.lra,
            reconciled.catalog_version,
            replication_factor,
            true,
            unit_clients,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn open_inner(
        timeline_id: i64,
        name: &str,
        term: i64,
        segments: Vec<Segment>,
        writable_segment: Segment,
        lrs: i64,
        lra: i64,
        catalog_version: i64,
        replication_factor: usize,
        needs_trunc: bool,
        unit_clients: &HashMap<String, UnitClient>,
    ) -> Result<Self, ChronicleError> {
        let inner = Arc::new(Mutex::new(InnerState {
            timeline_id,
            name: name.to_string(),
            term,
            segments,
            writable_segment: Some(writable_segment.clone()),
            lrs,
            lra,
            acked: HashMap::new(),
            catalog_version,
            replication_factor,
            needs_trunc,
            senders: HashMap::new(),
        }));

        let mut tasks = Vec::new();
        let mut senders = HashMap::new();

        for endpoint in &writable_segment.ensemble {
            let client = unit_clients.get(endpoint).ok_or_else(|| {
                ChronicleError::EnsembleUnavailable(format!("no client for unit {}", endpoint))
            })?;

            let (tx, response_stream) = client.open_record_stream(64).await?;
            senders.insert(endpoint.clone(), tx);

            let inner_clone = inner.clone();
            let ep = endpoint.clone();
            let client_clone = client.clone();
            tasks.push(tokio::spawn(async move {
                ack_collector(inner_clone, ep, client_clone, response_stream).await;
            }));
        }

        {
            let mut state = inner.lock().unwrap();
            state.senders = senders;
        }

        Ok(Self {
            timeline_id,
            inner,
            _tasks: tasks,
        })
    }

    pub fn timeline_id(&self) -> i64 {
        self.timeline_id
    }

    pub async fn handle_ensemble_change(
        &self,
        catalog: &dyn Catalog,
        unit_clients: &HashMap<String, UnitClient>,
        failed_units: &[String],
    ) -> Result<(), ChronicleError> {
        let available: Vec<String> = unit_clients.keys().cloned().collect();
        let exclude: Vec<String> = failed_units.to_vec();

        let (new_seg, catalog_version, name) = {
            let mut state = self.inner.lock().unwrap();
            let rf = state.replication_factor;

            let real_ensemble = select_ensemble(&available, &[], &exclude, rf).ok_or_else(|| {
                ChronicleError::EnsembleUnavailable("cannot find replacement ensemble".into())
            })?;

            let new_seg = Segment {
                id: state.segments.last().map_or(1, |s| s.id + 1),
                ensemble: real_ensemble,
                start_offset: state.lra + 1,
            };

            state.segments.push(new_seg.clone());
            state.writable_segment = Some(new_seg.clone());

            for (_offset, acked_set) in state.acked.iter_mut() {
                for fu in failed_units {
                    acked_set.remove(fu);
                }
            }

            for fu in failed_units {
                state.senders.remove(fu);
            }

            (new_seg, state.catalog_version, state.name.clone())
        };

        let tc = catalog.get_timeline(&name).await?;
        let mut updated = tc.clone();
        updated.segments.push(new_seg.clone());
        catalog.put_timeline(&updated, catalog_version).await?;

        for endpoint in &new_seg.ensemble {
            let has_sender = {
                let state = self.inner.lock().unwrap();
                state.senders.contains_key(endpoint)
            };
            if !has_sender {
                if let Some(client) = unit_clients.get(endpoint) {
                    let (tx, response_stream) = client.open_record_stream(64).await?;
                    {
                        let mut state = self.inner.lock().unwrap();
                        state.senders.insert(endpoint.clone(), tx);
                    }
                    let inner_clone = self.inner.clone();
                    let ep = endpoint.clone();
                    let client_clone = client.clone();
                    tokio::spawn(async move {
                        ack_collector(inner_clone, ep, client_clone, response_stream).await;
                    });
                }
            }
        }

        info!(
            ensemble = ?new_seg.ensemble,
            segment_id = new_seg.id,
            "ensemble change completed"
        );

        Ok(())
    }
}

#[async_trait::async_trait]
impl Writer for Timeline {
    async fn record(&self, event: UserEvent) -> Result<RecordResult, ChronicleError> {
        let (offset, request, senders) = {
            let mut state = self.inner.lock().unwrap();

            let ensemble = state
                .writable_segment
                .as_ref()
                .ok_or_else(|| ChronicleError::Internal("no writable segment".into()))?
                .ensemble
                .clone();

            let offset = state.lrs + 1;
            state.lrs = offset;

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;

            let proto_event = Event {
                timeline_id: state.timeline_id,
                term: state.term,
                offset,
                payload: Some(event.payload.into()),
                crc32: None,
                timestamp: now,
                schema_id: event.schema_id.unwrap_or(0),
            };

            let item = RecordEventsRequestItem {
                event: Some(proto_event),
                trunc: state.needs_trunc,
                lra: state.lra,
            };
            state.needs_trunc = false;

            state.acked.insert(offset, HashSet::new());

            let senders: Vec<mpsc::Sender<RecordEventsRequest>> = ensemble
                .iter()
                .filter_map(|ep| state.senders.get(ep).cloned())
                .collect();

            (offset, RecordEventsRequest { items: vec![item] }, senders)
        };

        for sender in &senders {
            if let Err(e) = sender.send(request.clone()).await {
                warn!(offset, error = %e, "failed to send record to unit");
            }
        }

        Ok(RecordResult {
            timeline_id: self.timeline_id,
            offset,
        })
    }
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

async fn ack_collector(
    inner: Arc<Mutex<InnerState>>,
    endpoint: String,
    client: UnitClient,
    initial_stream: tonic::Streaming<chronicle_proto::pb_ext::RecordEventsResponse>,
) {
    let mut client = client;
    let mut backoff = Duration::from_millis(100);
    const MAX_BACKOFF: Duration = Duration::from_secs(10);
    let mut response_stream = initial_stream;

    loop {
        while let Ok(Some(response)) = response_stream.message().await {
            let mut state = inner.lock().unwrap();

            for item in &response.items {
                if item.code == StatusCode::Ok as i32 {
                    if let Some(ref event) = item.event {
                        state
                            .acked
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

            let rf = state.replication_factor;
            while state.lra < state.lrs {
                let next = state.lra + 1;
                let fully_acked = state
                    .acked
                    .get(&next)
                    .is_some_and(|set| set.len() >= rf);
                if fully_acked {
                    state.lra = next;
                } else {
                    break;
                }
            }

            let lra = state.lra;
            state.acked.retain(|&offset, _| offset > lra);
        }

        {
            let mut state = inner.lock().unwrap();
            state.senders.remove(&endpoint);
        }
        warn!(endpoint = %endpoint, "record stream ended, reconnecting");

        loop {
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(MAX_BACKOFF);
            if let Err(re) = client.reconnect().await {
                warn!(endpoint = %endpoint, error = %re, "reconnect failed");
                continue;
            }
            match client.open_record_stream(64).await {
                Ok((tx, stream)) => {
                    backoff = Duration::from_millis(100);
                    {
                        let mut state = inner.lock().unwrap();
                        state.senders.insert(endpoint.clone(), tx);
                    }
                    response_stream = stream;
                    break;
                }
                Err(e) => {
                    warn!(endpoint = %endpoint, error = %e, "failed to reopen record stream");
                }
            }
        }
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
