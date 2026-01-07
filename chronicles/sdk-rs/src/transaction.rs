use crate::errors::ChronicleError;

struct Transaction {}

impl Transaction {
    async fn append(&self, payload: Vec<u8>) -> Result<i64, ChronicleError> {
        todo!()
    }

    async fn acknowledge_to(&self, offset: i64) -> Result<(), ChronicleError> {
        todo!()
    }
}
