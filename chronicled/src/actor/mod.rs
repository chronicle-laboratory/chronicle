use tokio::sync::mpsc::Sender;

pub mod read_handle_actor;
pub mod read_handle_group;
pub mod write_handle_actor;
pub mod write_handle_group;

pub struct Envelope<Req, Resp, RespError> {
    pub request: Req,
    pub res_tx: Sender<Result<Resp, RespError>>,
}
