use thiserror::Error;

#[derive(Error, Debug)]
pub enum ChronicleError {
    
    #[error("Unsupported Operation {0}")]
    Unsupported(String),
}
