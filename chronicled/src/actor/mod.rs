use tokio::sync::mpsc::Sender;

mod read_handle_actor;
mod write_handle_actor;

pub struct Envelope<Req, Resp, RespError> {
    pub request: Req,
    pub res_tx: Sender<Result<Resp, RespError>>,
}