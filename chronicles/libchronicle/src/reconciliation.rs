use crate::error::ChronicleError;
use crate::unit_client::UnitClient;
use tracing::info;

pub struct Reconciler;

impl Reconciler {
    pub async fn fence_all(
        clients: &mut [UnitClient],
        timeline_id: i64,
        new_term: i64,
    ) -> Result<i64, ChronicleError> {
        let mut max_lra = -1i64;

        for client in clients.iter_mut() {
            match client.fence(timeline_id, new_term).await {
                Ok(resp) => {
                    if resp.lra > max_lra {
                        max_lra = resp.lra;
                    }
                }
                Err(e) => {
                    return Err(ChronicleError::UnitUnavailable(e.to_string()));
                }
            }
        }

        info!(
            timeline_id,
            new_term, max_lra, "fenced all units in ensemble"
        );
        Ok(max_lra)
    }
}
