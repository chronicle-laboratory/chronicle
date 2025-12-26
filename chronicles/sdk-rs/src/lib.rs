#[allow(clippy::derive_partial_eq_without_eq)]
pub mod pb_external {
    include!(concat!(env!("OUT_DIR"), "/pb_external.rs"));
}

#[allow(clippy::derive_partial_eq_without_eq)]
pub mod pb_storage {
    include!(concat!(env!("OUT_DIR"), "/pb_storage.rs"));
}
