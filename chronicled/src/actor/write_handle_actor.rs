use crate::actor::Envelope;
use crate::error::unit_error::UnitError;
use crate::wal::wal::Wal;
use chronicle_proto::pb_ext::{RecordEventsRequest, RecordEventsRequestItem, RecordEventsResponse};
use prost::{EncodeError, Message};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_util::bytes::BytesMut;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use chronicle_proto::pb_storage::BatchedRaw;

struct WriteActorGroup {
    actors: Vec<WriteActor>,
    actor_id_mask: i64,
}

impl WriteActorGroup {
    pub fn new(actor_num: usize) -> Self {
        let actors = Vec::with_capacity(actor_num);
        // init actors
        Self {
            actors,
            actor_id_mask: (actor_num - 1) as i64,
        }
    }

    #[inline]
    pub async fn dispatch(
        &self,
        envelope: Envelope<RecordEventsRequestItem, RecordEventsResponse, Status>,
    ) -> Result<(), Status> {
        match &envelope.request.event {
            Some(event) => {
                let idx = (event.timeline_id & self.actor_id_mask) as usize;
                self.actors[idx]
                    .send(envelope)
                    .await
                    .map_err(|_| Status::unavailable("write handle actor is unavailable."))?
            }
            _ => return Err(Status::invalid_argument("unexpect empty event.")),
        }
        Ok(())
    }
}
struct WriteActor {
    context: CancellationToken,
    mailbox: Sender<Envelope<RecordEventsRequestItem, RecordEventsResponse, Status>>,
    worker_handle: JoinHandle<()>,

    write_ahead_log: Wal,
}
impl WriteActor {
    fn new(inflight_num: usize) -> Self {
        let context = CancellationToken::new();
        let (mailbox_tx, mut mailbox_rx) = tokio::sync::mpsc::channel(inflight_num);
        let handle = tokio::spawn(Self::bg_worker(context.clone(), mailbox_rx));
        Self {
            context,
            mailbox: mailbox_tx,
            worker_handle: handle,
        }
    }

    async fn bg_worker(
        context: CancellationToken,
        mut mailbox_rx: Receiver<Envelope<RecordEventsRequestItem, RecordEventsResponse, Status>>,
    ) {
        let mut vec = Vec::new();
        while !context.is_cancelled() {
            let _ = mailbox_rx.recv_many(&mut vec, 1024).await;
            for envelope in vec.drain(..) {
                let record_events_request_item = envelope.request;

            }
        }
    }

    async fn send(
        &self,
        event: Envelope<RecordEventsRequestItem, RecordEventsResponse, Status>,
    ) -> Result<(), UnitError> {
        self.mailbox.send(event).await.map_err(|err| {
            UnitError::Unavailable(format!("unexpected actor mailbox status. {:?}", err))
        })
    }
}
