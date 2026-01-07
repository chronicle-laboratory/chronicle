use crate::actor::Envelope;
use crate::error::unit_error::UnitError;
use crate::storage::TimelineReader;
use crate::storage::storage::Storage;
use crate::storage::write_cache::WriteCache;
use crate::wal::wal::Wal;
use chronicle_proto::pb_ext::{
    ChunkType, Event, FetchEventsRequest, FetchEventsResponse, RecordEventsRequestItem, StatusCode,
};
use log::{info, warn};
use prost::Message;
use std::collections::VecDeque;
use tokio::io::AsyncSeek;
use tokio::select;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::codegen::tokio_stream::StreamExt;
use tonic::{Status, include_file_descriptor_set};

pub struct ReadActor {
    pub context: CancellationToken,
    pub worker_handle: JoinHandle<()>,
    pub mailbox: Sender<Envelope<FetchEventsRequest, FetchEventsResponse, Status>>,
}

impl ReadActor {
    pub fn new(index: i32, inflight_num: usize, storage: Storage) -> Self {
        let context = CancellationToken::new();
        let (mailbox_tx, mailbox_rx) = tokio::sync::mpsc::channel(inflight_num);
        let handle = tokio::spawn(Self::bg_worker(context.clone(), mailbox_rx, storage));
        Self {
            context,
            worker_handle: handle,
            mailbox: mailbox_tx,
        }
    }

    async fn bg_worker(
        context: CancellationToken,
        mut mailbox_rx: Receiver<Envelope<FetchEventsRequest, FetchEventsResponse, Status>>,
        storage: Storage,
    ) {
        let mut inflight_read = Vec::new();
        loop {
            select! {
                _ = context.cancelled() =>  {
                    info!("bg_worker exit");
                    break;
                }
                count = mailbox_rx.recv_many(&mut inflight_read, 1024) => {
                    if count == 0 {
                        info!("mailbox closed, bg_worker exit");
                        break;
                    }
                   'envelop_loop: for envelope in inflight_read.drain(..) {
                        let request = envelope.request;
                        let e_offset = request.end_offset;
                        let mut stream = storage.fetch_batches(request.timeline_id, request.start_offset, request.end_offset);

                        let mut latest_advanced_offset = -1;
                        let mut send_first = false;
                        while let Some((offset, advanced_offset, events)) = stream.next().await {
                            latest_advanced_offset = advanced_offset;
                            let mut res = FetchEventsResponse {
                                code: StatusCode::Ok.into(),
                                r#type: -1,
                                timeline_id: request.timeline_id,
                                event: events,
                                advanced_offset,
                            };
                            let mut no_more = false;
                            if offset != e_offset {
                                if !send_first {
                                    res.r#type = ChunkType::First.into();
                                    send_first = true;
                                } else {
                                    res.r#type = ChunkType::Middle.into();
                                }
                            } else {
                                no_more = true;
                                if !send_first {
                                    res.r#type = ChunkType::Full.into();
                                } else {
                                    res.r#type = ChunkType::Last.into();
                                }
                            }
                            if let Err(err) = envelope.res_tx.send(Ok(res)).await {
                                warn!("Send response to the client failed. {:?}", err);
                                continue 'envelop_loop;
                            }
                            if no_more {
                                continue 'envelop_loop;
                            }
                        }
                        // if no more data
                        let mut res = FetchEventsResponse {
                            code: StatusCode::Ok.into(),
                            r#type: -1,
                            timeline_id: request.timeline_id,
                            event: vec![],
                            advanced_offset: latest_advanced_offset,
                        };
                        if send_first {
                            // we didn't send the last
                            res.r#type = ChunkType::Last.into();
                        } else {
                            // we didn't send anything
                            res.r#type = ChunkType::Full.into();
                        }
                        if let Err(err) = envelope.res_tx.send(Ok(res)).await {
                            warn!("Send response to the client failed. {:?}", err);
                        }
                   }
                }
            }
        }
    }

    pub async fn send(
        &self,
        event: Envelope<FetchEventsRequest, FetchEventsResponse, Status>,
    ) -> Result<(), UnitError> {
        self.mailbox.send(event).await.map_err(|err| {
            UnitError::Unavailable(format!("unexpected actor mailbox status. {:?}", err))
        })
    }
}
