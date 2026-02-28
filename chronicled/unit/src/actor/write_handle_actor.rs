use crate::actor::Envelope;
use crate::error::unit_error::UnitError;
use crate::storage::storage::Storage;
use crate::storage::write_cache::WriteCache;
use crate::unit::timeline_state::TimelineStateManager;
use crate::wal::wal::Wal;
use chronicle_proto::pb_ext::{Event, RecordEventsRequestItem};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tonic::Status;

pub struct WriteActor {
    pub context: CancellationToken,
    pub worker_handle: JoinHandle<()>,
    pub mailbox: Sender<Envelope<RecordEventsRequestItem, Event, Status>>,
}

impl WriteActor {
    pub fn new(
        _index: i32,
        inflight_num: usize,
        wal: Wal,
        storage: Storage,
        timeline_state: Arc<TimelineStateManager>,
    ) -> Self {
        let context = CancellationToken::new();
        let (mailbox_tx, mailbox_rx) = tokio::sync::mpsc::channel(inflight_num);
        let wc = storage.fetch_write_cache();
        let handle = tokio::spawn(Self::bg_worker(
            context.clone(),
            mailbox_rx,
            wal,
            wc,
            timeline_state,
        ));
        Self {
            context,
            mailbox: mailbox_tx,
            worker_handle: handle,
        }
    }

    async fn bg_worker(
        context: CancellationToken,
        mut mailbox_rx: Receiver<Envelope<RecordEventsRequestItem, Event, Status>>,
        wal: Wal,
        write_cache: WriteCache,
        timeline_state: Arc<TimelineStateManager>,
    ) {
        let mut inflight_synced: VecDeque<(
            i64,
            Envelope<RecordEventsRequestItem, Event, Status>,
        )> = VecDeque::new();
        let mut inflight_append = Vec::new();
        let mut watch_monitor = wal.watch_synced();

        loop {
            select! {
                _ = context.cancelled() => {
                    info!("WriteActor bg_worker exit");
                    break;
                }
                _ = watch_monitor.changed() => {
                    let synced_offset = *watch_monitor.borrow();
                    let new_head = inflight_synced.partition_point(|(offset, _)| *offset <= synced_offset);
                    for (_, envelope) in inflight_synced.drain(0..new_head) {
                        let request = envelope.request;
                        let event = request.event.unwrap();
                        let timeline_id = event.timeline_id;
                        let term = event.term;
                        let offset = event.offset;
                        let lra = request.lra;

                        if let Err(e) = write_cache.put_with_trunc(event, request.trunc) {
                            warn!("Failed to write to storage: {:?}", e);
                        }

                        // Update LRA if provided
                        if lra >= 0 {
                            timeline_state.update_lra(timeline_id, lra);
                        }

                        if let Err(err) = envelope.res_tx.try_send(Ok(Event {
                            timeline_id,
                            term,
                            offset,
                            payload: None,
                            crc32: None,
                            timestamp: -1,
                        })) {
                            warn!("Send response to client failed: {:?}", err);
                        }
                    }
                }
                _ = mailbox_rx.recv_many(&mut inflight_append, 1024) => {
                    for envelope in inflight_append.drain(..) {
                        let record_item = &envelope.request;
                        if let Some(event) = &record_item.event {
                            // Check term before accepting write
                            if let Err(current_term) = timeline_state.check_term(event.timeline_id, event.term) {
                                let _ = envelope.res_tx.try_send(Err(Status::failed_precondition(
                                    format!("INVALID_TERM: current={}, request={}", current_term, event.term),
                                )));
                                continue;
                            }

                            if let Some(payload) = &event.payload {
                                match wal.append(payload.to_vec()).await {
                                    Ok(offset) => {
                                        inflight_synced.push_back((offset, envelope));
                                    }
                                    Err(error) => {
                                        if let Err(err) = envelope
                                            .res_tx
                                            .try_send(Err(Status::internal(error.to_string())))
                                        {
                                            warn!("Send response to client failed: {:?}", err);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    pub async fn send(
        &self,
        event: Envelope<RecordEventsRequestItem, Event, Status>,
    ) -> Result<(), UnitError> {
        self.mailbox.send(event).await.map_err(|err| {
            UnitError::Unavailable(format!("unexpected actor mailbox status. {:?}", err))
        })
    }
}
