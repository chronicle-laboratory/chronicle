use tokio::sync::mpsc::Sender;

mod read_handle_actor;
mod write_handle_actor;
mod write_handle_group;
mod read_handle_group;

pub struct Envelope<Req, Resp, RespError> {
    pub request: Req,
    pub res_tx: Sender<Result<Resp, RespError>>,
}