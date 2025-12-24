use crate::storage::storage::Storage;
use crate::wal::wal::Wal;
use chronicle_proto::pb_ext::chronicle_server::Chronicle;
use chronicle_proto::pb_ext::{
    FenceRequest, FenceResponse, FetchEventsRequest, FetchEventsResponse, RecordEventsRequest,
    RecordEventsResponse, TrimRequest, TrimResponse,
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

    type FetchStream = BoxStream<FetchEventsResponse>;

    async fn fetch(
        &self,
        request: Request<Streaming<FetchEventsRequest>>,
    ) -> Result<Response<Self::FetchStream>, Status> {
        todo!()
    }

    async fn fence(
        &self,
        request: Request<FenceRequest>,
    ) -> Result<Response<FenceResponse>, Status> {
        todo!()
    }

    async fn trim(&self, request: Request<TrimRequest>) -> Result<Response<TrimResponse>, Status> {
        todo!()
    }
}
