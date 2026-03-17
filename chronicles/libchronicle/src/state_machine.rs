use crate::conn::{Conn, ConnPool};
use crate::error::ChronicleError;
use crate::Offset;
use catalog::Catalog;
use chronicle_proto::pb_catalog::{Segment, TimelineMeta, TimelineStatus};
use chronicle_proto::pb_ext::{
    RecordEventsRequest, RecordEventsRequestItem, StatusCode,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

pub(crate) struct StateMachine {
    inner: Arc<Mutex<Inner>>,
    pub segments: Vec<Segment>,
    pub ack_tasks: Vec<tokio::task::JoinHandle<()>>,
}

struct Inner {
    timeline_id: i64,
    term: i64,
    lrs: i64,
    lra: i64,
    replication_factor: usize,
    #[allow(dead_code)]
    schema_id: Option<String>,
    needs_trunc: bool,
    acked: HashMap<i64, HashSet<String>>,
    waiters: HashMap<i64, oneshot::Sender<Result<Offset, ChronicleError>>>,
    senders: HashMap<String, mpsc::Sender<RecordEventsRequest>>,
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
        tc: &TimelineMeta,
        replication_factor: usize,
        schema_id: Option<String>,
    ) -> Result<Self, ChronicleError> {
        let last_segment = catalog.get_last_segment(&tc.name).await?
            .ok_or_else(|| {
                ChronicleError::ReconciliationFailed("timeline has no segments".into())
            })?;
        let ensemble = &last_segment.ensemble;

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
        for endpoint in ensemble {
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
        let mut segments: Vec<Segment> = existing_segments
            .into_iter()
            .filter(|seg| seg.start_offset <= lra)
            .collect();

        let writable_segment = Segment {
            ensemble: ensemble.clone(),
            start_offset: lra + 1,
        };
        catalog.put_segment(&tc.name, &writable_segment).await?;
        segments.push(writable_segment.clone());

        let mut updated = tc.clone();
        updated.status = TimelineStatus::Active as i32;
        updated.lra = lra;
        catalog.put_timeline(&updated, tc.version).await?;

        info!(
            timeline_id = tc.timeline_id,
            term = new_term,
            lra = lra,
            "new term: complete"
        );

        let inner = Arc::new(Mutex::new(Inner {
            timeline_id: tc.timeline_id,
            term: new_term,
            lrs: lra,
            lra,
            replication_factor,
            schema_id,
            needs_trunc: lra > 0,
            acked: HashMap::new(),
            waiters: HashMap::new(),
            senders: HashMap::new(),
        }));

        let mut record_senders = HashMap::new();
        let mut ack_tasks = Vec::new();

        for (endpoint, conn) in &conns {
            let (tx, response_stream) = conn.open_record_stream(64).await?;
            record_senders.insert(endpoint.clone(), tx);

            let inner_clone = inner.clone();
            let ep = endpoint.clone();
            ack_tasks.push(tokio::spawn(async move {
                ack_collector(inner_clone, ep, response_stream).await;
            }));
        }

        {
            let mut s = inner.lock().unwrap();
            s.senders = record_senders;
        }

        Ok(Self {
            inner,
            segments,
            ack_tasks,
        })
    }

    pub async fn close(&self) {
        let mut s = self.inner.lock().unwrap();
        s.senders.clear();
    }

    pub fn lra(&self) -> i64 {
        self.inner.lock().unwrap().lra
    }

    pub fn prepare_batch(
        &self,
        timeline_id: i64,
        events: Vec<(crate::Event, oneshot::Sender<Result<Offset, ChronicleError>>)>,
    ) -> Vec<RecordEventsRequestItem> {
        let mut s = self.inner.lock().unwrap();
        let mut items = Vec::with_capacity(events.len());

        for (event, tx) in events {
            let offset = s.lrs + 1;
            s.lrs = offset;

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;

            let proto_event = chronicle_proto::pb_ext::Event {
                timeline_id,
                term: s.term,
                offset,
                payload: Some(event.payload.into()),
                crc32: None,
                timestamp: now,
                schema_id: 0,
            };

            let item = RecordEventsRequestItem {
                event: Some(proto_event),
                trunc: s.needs_trunc,
                lra: s.lra,
            };
            s.needs_trunc = false;

            s.acked.insert(offset, HashSet::new());
            s.waiters.insert(offset, tx);

            items.push(item);
        }

        items
    }

    pub async fn replicate(&self, items: Vec<RecordEventsRequestItem>) -> Result<(), ChronicleError> {
        let senders = {
            let s = self.inner.lock().unwrap();
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
    inner: Arc<Mutex<Inner>>,
    endpoint: String,
    response_stream: tonic::Streaming<chronicle_proto::pb_ext::RecordEventsResponse>,
) {
    let mut response_stream = response_stream;

    while let Ok(Some(response)) = response_stream.message().await {
        let mut s = inner.lock().unwrap();

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
        let mut s = inner.lock().unwrap();
        s.senders.remove(&endpoint);
    }
    warn!(endpoint = %endpoint, "record stream ended");
}
