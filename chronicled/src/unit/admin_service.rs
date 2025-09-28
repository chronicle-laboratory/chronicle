use chronicle_proto::pb_admin::admin_server::Admin;

pub struct AdminService;

#[tonic::async_trait]
impl Admin for AdminService {}
