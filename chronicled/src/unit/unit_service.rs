use crate::actor::read_handle_group::ReadHandleGroup;
use crate::actor::write_handle_group::WriteActorGroup;
use crate::actor::Envelope;
use crate::unit::timeline_state::TimelineStateManager;
use chronicle_proto::pb_ext::chronicle_server::Chronicle;
use chronicle_proto::pb_ext::{
    FenceRequest, FenceResponse, FetchEventsRequest, FetchEventsResponse,
    RecordEventsRequest, RecordEventsResponse, RecordEventsResponseItem, StatusCode,
};
use futures_util::StreamExt;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::BoxStream;
use tonic::{Request, Response, Status, Streaming};

pub struct UnitService {
    write_group: Arc<WriteActorGroup>,
    read_group: Arc<ReadHandleGroup>,
    timeline_state: Arc<TimelineStateManager>,
}

impl UnitService {
    pub fn new(
        write_group: Arc<WriteActorGroup>,
        read_group: Arc<ReadHandleGroup>,
        timeline_state: Arc<TimelineStateManager>,
    ) -> Self {
        Self {
            write_group,
            read_group,
            timeline_state,
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
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(4);
        let write_group = self.write_group.clone();

        tokio::spawn(async move {
            while let Some(request) = stream.next().await {
                match request {
                    Ok(req) => {
                        for item in req.items {
                            let (item_tx, mut item_rx) = mpsc::channel(1);
                            let envelope = Envelope {
                                request: item,
                                res_tx: item_tx,
                            };
                            if let Err(e) = write_group.dispatch(envelope).await {
                                let response = RecordEventsResponse {
                                    items: vec![RecordEventsResponseItem {
                                        code: StatusCode::InvalidTerm.into(),
                                        event: None,
                                    }],
                                };
                                if tx.send(Ok(response)).await.is_err() {
                                    return;
                                }
                                continue;
                            }
                            // Wait for actor response
                            if let Some(result) = item_rx.recv().await {
                                match result {
                                    Ok(event) => {
                                        let response = RecordEventsResponse {
                                            items: vec![RecordEventsResponseItem {
                                                code: StatusCode::Ok.into(),
                                                event: Some(event),
                                            }],
                                        };
                                        if tx.send(Ok(response)).await.is_err() {
                                            return;
                                        }
                                    }
                                    Err(status) => {
                                        if tx.send(Err(status)).await.is_err() {
                                            return;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        if tx.send(Err(e)).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::RecordStream,
        ))
    }

    type FetchStream = BoxStream<FetchEventsResponse>;

    async fn fetch(
        &self,
        request: Request<Streaming<FetchEventsRequest>>,
    ) -> Result<Response<Self::FetchStream>, Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(4);
        let read_group = self.read_group.clone();

        tokio::spawn(async move {
            while let Some(request) = stream.next().await {
                match request {
                    Ok(req) => {
                        let (item_tx, mut item_rx) = mpsc::channel(16);
                        let envelope = Envelope {
                            request: req,
                            res_tx: item_tx,
                        };
                        if let Err(e) = read_group.dispatch(envelope).await {
                            if tx.send(Err(e)).await.is_err() {
                                return;
                            }
                            continue;
                        }
                        // Forward actor responses
                        while let Some(result) = item_rx.recv().await {
                            if tx.send(result).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        if tx.send(Err(e)).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::FetchStream,
        ))
    }

    async fn fence(
        &self,
        request: Request<FenceRequest>,
    ) -> Result<Response<FenceResponse>, Status> {
        let req = request.into_inner();
        match self.timeline_state.fence(req.timeline_id, req.term) {
            Ok(lra) => Ok(Response::new(FenceResponse {
                code: StatusCode::Ok.into(),
                lra,
                term: req.term,
            })),
            Err(current_term) => Ok(Response::new(FenceResponse {
                code: StatusCode::Fenced.into(),
                lra: -1,
                term: current_term,
            })),
        }
    }
}
