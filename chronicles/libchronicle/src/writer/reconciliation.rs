use crate::client::unit_client::UnitClient;
use crate::error::ChronicleError;
use catalog::Catalog;
use chronicle_proto::pb_catalog::{Segment, TimelineStatus};
use chronicle_proto::pb_ext::StatusCode;
use std::collections::HashMap;
use tracing::{info, warn};

/// Result of a successful reconciliation: the timeline is fenced, dirty entries
/// identified, and the catalog updated with a new writable segment.
#[derive(Debug, Clone)]
pub struct ReconciledState {
    pub term: i64,
    pub lra: i64,
    pub lrs: i64,
    pub segments: Vec<Segment>,
    pub writable_segment: Segment,
    pub catalog_version: i64,
}

/// Run the full reconciliation protocol for an existing timeline.
///
/// Implements TLA+ actions: `TimelineStartReconciliation`,
/// `TimelineHandleFenceResponse`, `TimelineRetryFenceRequest`,
/// `TimelineCompleteReconciliation`.
///
/// **Phase 1 — Start**: Read catalog, increment term, CAS.
/// **Phase 2 — Fence**: Send Fence to ALL units in last segment, collect LRAs.
/// **Phase 3 — Complete**: Rebuild segments, CAS catalog with Open status.
pub async fn reconcile(
    catalog: &dyn Catalog,
    unit_clients: &HashMap<String, UnitClient>,
    timeline_name: &str,
) -> Result<ReconciledState, ChronicleError> {
    // ── Phase 1: Start — increment term in catalog ──────────────────────
    let mut tc = catalog.get_timeline(timeline_name).await?;
    let new_term = tc.term + 1;
    tc.term = new_term;
    let tc = catalog.put_timeline(&tc, tc.version).await?;

    info!(
        timeline = timeline_name,
        timeline_id = tc.timeline_id,
        term = new_term,
        "reconciliation: starting"
    );

    // Last segment's ensemble are the units we must fence.
    let last_segment = tc.segments.last().ok_or_else(|| {
        ChronicleError::ReconciliationFailed("timeline has no segments".into())
    })?;
    let ensemble = &last_segment.ensemble;

    // ── Phase 2: Fence — fence ALL units, collect max LRA ───────────────
    let mut max_lra: i64 = 0;

    for endpoint in ensemble {
        let client = unit_clients.get(endpoint).ok_or_else(|| {
            ChronicleError::EnsembleUnavailable(format!("no client for unit {}", endpoint))
        })?;

        // Retry with backoff for transient failures.
        let mut attempts: u64 = 0;
        let response = loop {
            attempts += 1;
            match client.fence(tc.timeline_id, new_term).await {
                Ok(resp) => break resp,
                Err(e) if attempts < 5 => {
                    warn!(
                        endpoint = endpoint.as_str(),
                        attempt = attempts,
                        error = %e,
                        "reconciliation: fence request failed, retrying"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(100 * attempts)).await;
                }
                Err(e) => {
                    return Err(ChronicleError::ReconciliationFailed(format!(
                        "failed to fence unit {}: {}",
                        endpoint, e
                    )));
                }
            }
        };

        if response.code == StatusCode::Ok as i32 {
            info!(
                endpoint = endpoint.as_str(),
                lra = response.lra,
                "reconciliation: unit fenced"
            );
            if response.lra > max_lra {
                max_lra = response.lra;
            }
        } else if response.code == StatusCode::Fenced as i32 {
            return Err(ChronicleError::Fenced {
                timeline_id: tc.timeline_id,
                term: response.term,
            });
        } else {
            return Err(ChronicleError::InvalidTerm {
                current: response.term,
                requested: new_term,
            });
        }
    }

    // ── Phase 3: Complete — rebuild segments, update catalog ─────────────
    let new_lra = max_lra;

    // Keep segments whose start_offset falls within committed range.
    let mut new_segments: Vec<Segment> = tc
        .segments
        .iter()
        .filter(|seg| seg.start_offset <= new_lra)
        .cloned()
        .collect();

    // Append writable segment starting right after the last committed offset.
    let writable_segment = Segment {
        id: new_segments.last().map_or(1, |s| s.id + 1),
        ensemble: ensemble.clone(),
        start_offset: new_lra + 1,
    };
    new_segments.push(writable_segment.clone());

    // CAS catalog with Open status, updated segments, new LRA.
    let mut updated = tc.clone();
    updated.status = TimelineStatus::Active as i32;
    updated.segments = new_segments.clone();
    updated.lra = new_lra;
    let updated = catalog.put_timeline(&updated, tc.version).await?;

    info!(
        timeline = timeline_name,
        term = new_term,
        lra = new_lra,
        segments = updated.segments.len(),
        "reconciliation: complete"
    );

    Ok(ReconciledState {
        term: new_term,
        lra: new_lra,
        lrs: new_lra,
        segments: updated.segments,
        writable_segment,
        catalog_version: updated.version,
    })
}
