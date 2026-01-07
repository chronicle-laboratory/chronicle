use crate::actor::read_handle_actor::ReadActor;
use crate::actor::Envelope;
use crate::storage::storage::Storage;
use chronicle_proto::pb_ext::{FetchEventsRequest, FetchEventsResponse};
use tonic::Status;

struct ReadHandleGroup {
    actors: Vec<ReadActor>,
    actor_id_mask: i64,
}

impl ReadHandleGroup {
    pub fn new(actor_num: usize, inflight_num: usize, storage: Storage) -> Self {
        let mut actors = Vec::with_capacity(actor_num);
        let inflight_per_actor = inflight_num / actor_num;

        // init actors container
        for index in 0..actor_num {
            let actor = ReadActor::new(index as i32, inflight_per_actor, storage.clone());
            actors[index] = actor
        }

        Self {
            actors,
            actor_id_mask: (actor_num - 1) as i64,
        }
    }

    #[inline]
    pub async fn dispatch(
        &self,
        envelope: Envelope<FetchEventsRequest, FetchEventsResponse, Status>,
    ) -> Result<(), Status> {
        match &envelope.request {
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
