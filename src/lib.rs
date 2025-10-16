#[allow(clippy::derive_partial_eq_without_eq)]
pub mod pb_external {
    include!(concat!(env!("OUT_DIR"), "/pb_external.rs"));
}
#[allow(clippy::derive_partial_eq_without_eq)]
pub mod pb_metadata {
    include!(concat!(env!("OUT_DIR"), "/pb_metadata.rs"));
}
#[allow(clippy::derive_partial_eq_without_eq)]
pub mod pb_storage {
    include!(concat!(env!("OUT_DIR"), "/pb_storage.rs"));
}
mod banner;
mod error;
mod log;
mod metadata;
mod service;
mod storage;
pub mod unit;
