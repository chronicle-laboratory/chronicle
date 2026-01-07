use crate::actor::Envelope;
use crate::error::unit_error::UnitError;
use crate::storage::storage::Storage;
use crate::storage::write_cache::WriteCache;
use crate::wal::wal::Wal;
use chronicle_proto::pb_ext::{Event, RecordEventsRequestItem};
use hyper::body::Bytes;
use log::{info, warn};
use memberlist::proto::Data;
use prost::Message;
use prost::bytes::BytesMut;
use std::collections::VecDeque;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::Status;

pub struct WriteActor {
    pub context: CancellationToken,
    pub worker_handle: JoinHandle<()>,
    pub mailbox: Sender<Envelope<RecordEventsRequestItem, Event, Status>>,
}
impl WriteActor {
    pub fn new(index: i32, inflight_num: usize, wal: Wal, storage: Storage) -> Self {
        let context = CancellationToken::new();
        let (mailbox_tx, mut mailbox_rx) = tokio::sync::mpsc::channel(inflight_num);
        let wc = storage.fetch_write_cache(index);
        let handle = tokio::spawn(Self::bg_worker(context.clone(), mailbox_rx, wal, wc));
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
    ) {
        let mut inflight_synced: VecDeque<(i64, Envelope<RecordEventsRequestItem, Event, Status>)> =
            VecDeque::new();
        let mut inflight_append = Vec::new();
        let mut watch_monitor = wal.watch_synced();
        loop {
            select! {
                _ = context.cancelled() =>  {
                    info!("bg_worker exit");
                }
                _ = watch_monitor.changed() => {
                    let synced_offset = *watch_monitor.borrow();
                    let new_head = inflight_synced.partition_point(|(offset, _)| *offset <= synced_offset);
                    for (_, envelop) in inflight_synced.drain(0..new_head) {
                        let request = envelop.request;
                        let event = request.event.unwrap();
                        let timeline_id = event.timeline_id;
                        let term = event.term;
                        let offset = event.offset;
                        write_cache.put_with_trunc(event, request.trunc);
                        if let Err(err) = envelop.res_tx.try_send(Ok(Event{timeline_id,term,offset,payload: None,crc32: None,timestamp: -1})) {
                            // todo: improve here
                            warn!("Send response to the client failed. {:?}", err);
                        }
                    }
                }
                _ = mailbox_rx.recv_many(&mut inflight_append, 1024) => {
                   for envelope in inflight_append.drain(..) {
                    let record_events_request_item = &envelope.request;
                        let bytes = BytesMut::new()

                        bytes.chain(record_events_request_item.event.unwrap().payload)
                        match wal.append(bytes).await {
                        Ok(offset) => {
                            inflight_synced.push_back((offset, envelope));
                        }
                        Err(error) => {
                             if let Err(err) =  envelope.res_tx.try_send(Err(Status::internal(error.to_string()))) {
                                warn!("Send response to the client failed. {:?}", err);
                            }
                        }}
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
