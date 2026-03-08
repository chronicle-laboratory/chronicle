pub mod pb_ext {
    tonic::include_proto!("io.chronicle.proto.ext.v1");
}
pub mod pb_storage {
    tonic::include_proto!("io.chronicle.proto.storage.v1");
}
pub mod pb_catalog {
    tonic::include_proto!("io.chronicle.proto.catalog.v1");
}
pub mod pb_admin {
    tonic::include_proto!("io.chronicle.proto.admin.v1");
}
pub mod pb_saga {
    tonic::include_proto!("io.chronicle.proto.saga.v1");
}
pub mod pb_lexicon {
    tonic::include_proto!("chronicle.lexicon");
}
