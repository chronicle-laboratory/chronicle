use crate::storage::storage::Storage;
use crate::wal::wal::Wal;
use chronicle_proto::pb_ext::chronicle_server::Chronicle;
use chronicle_proto::pb_ext::{
    FetchEventsRequest, FetchEventsResponse, RecordEventsRequest, RecordEventsResponse,
};
use tonic::codegen::BoxStream;
use tonic::{Request, Response, Status, Streaming};

pub struct UnitService {
    storage: Storage,
    write_ahead_log: Wal,
}

impl UnitService {
    pub fn new(storage: Storage, write_ahead_log: Wal) -> Self {
        Self {
            storage,
            write_ahead_log,
        }
    }
}

#[tonic::async_trait]
impl Chronicle for UnitService {
    type RecordStream = BoxStream<RecordEventsResponse>;

    async fn record(
        &self,
        request: Request<Streaming<RecordEventsRequest>>,
    ) -> Result<Response<Self::RecordStream>, Status> {
        todo!()
    }

    type FollowStream = BoxStream<FetchEventsResponse>;

    async fn follow(
        &self,
        request: Request<Streaming<FetchEventsRequest>>,
    ) -> Result<Response<Self::FollowStream>, Status> {
        todo!()
    }
}
