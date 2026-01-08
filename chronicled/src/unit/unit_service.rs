use crate::storage::storage::Storage;
use crate::storage::TimelineReader;
use crate::wal::wal::Wal;
use chronicle_catalog::catalog::Catalog;
use chronicle_proto::pb_ext::chronicle_server::Chronicle;
use chronicle_proto::pb_ext::{
    Event, FenceRequest, FenceResponse, FetchEventsRequest, FetchEventsResponse,
    RecordEventsRequest, RecordEventsRequestItem, RecordEventsResponse, RecordEventsResponseItem,
    TrimRequest, TrimResponse,
};
use futures_util::StreamExt;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::codegen::BoxStream;
use tonic::{Request, Response, Status, Streaming};

pub struct UnitService {
    storage: Storage,
    write_ahead_log: Wal,
    catalog: Arc<Box<dyn Catalog>>,
}

impl UnitService {
    pub fn new(storage: Storage, write_ahead_log: Wal, catalog: Arc<Box<dyn Catalog>>) -> Self {
        Self {
            storage,
            write_ahead_log,
            catalog,
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
        let wal = self.write_ahead_log.clone();

        tokio::spawn(async move {
            while let Some(request) = stream.next().await {
                match request {
                    Ok(req) => {
                        let mut response_items = Vec::new();
                        for item in req.items {
                            if let Some(event) = item.event {
                                let timeline_id = event.timeline_id;
                                let term = event.term;
                                if let Some(payload) = event.payload {
                                    match wal.append(payload.to_vec()).await {
                                        Ok(offset) => {
                                            response_items.push(RecordEventsResponseItem {
                                                code: 0, // OK
                                                event: Some(Event {
                                                    timeline_id,
                                                    term,
                                                    offset,
                                                    timestamp: 0, // TODO: use real timestamp
                                                    payload: None, // Don't send payload back
                                                    crc32: None,     // TODO: calculate crc32
                                                }),
                                            });
                                        }
                                        Err(e) => {
                                            // How to handle error for a single item?
                                            // For now, we just log it and don't add to response.
                                            eprintln!("Failed to append event: {}", e);
                                        }
                                    }
                                }
                            }
                        }
                        if !response_items.is_empty() {
                            let response = RecordEventsResponse {
                                items: response_items,
                            };
                            if tx.send(Ok(response)).await.is_err() {
                                break; // Downstream closed
                            }
                        }
                    }
                    Err(e) => {
                        if tx.send(Err(e)).await.is_err() {
                            break;
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
        let storage = self.storage.clone();

        tokio::spawn(async move {
            while let Some(request) = stream.next().await {
                match request {
                    Ok(req) => {
                        let mut data_stream =
                            storage.fetch_batches(req.timeline_id, req.start_offset, req.end_offset);

                        while let Some((_offset, advanced_offset, events)) =
                            data_stream.next().await
                        {
                            let response = FetchEventsResponse {
                                code: 0,   // Ok
                                r#type: 0, // TODO: Handle chunking (First, Middle, Last, Full)
                                timeline_id: req.timeline_id,
                                event: events,
                                advanced_offset,
                            };
                            if tx.send(Ok(response)).await.is_err() {
                                break; // Downstream closed
                            }
                        }
                    }
                    Err(e) => {
                        if tx.send(Err(e)).await.is_err() {
                            break;
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
        self.catalog
            .open_timeline("default".to_string(), req.timeline_id as u64, req.term as u64)
            .await;
        Ok(Response::new(FenceResponse { code: 0 }))
    }

    async fn trim(&self, _request: Request<TrimRequest>) -> Result<Response<TrimResponse>, Status> {
        todo!()
    }
}