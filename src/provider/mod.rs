use std::net::SocketAddr;

pub mod internal_provider_manager;

pub fn into_tonic_address(addr: SocketAddr, secure: bool) -> String {
    match secure {
        true => format!("https://{}", addr),
        false => format!("http://{}", addr),
    }
}
