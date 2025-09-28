use crate::catalog::Catalog;
use crate::error::ChronicleError;
use chronicle_proto::pb_catalog::{Segment, TimelineCatalog};

pub async fn change_ensemble(
    catalog: &dyn Catalog,
    timeline_catalog: &TimelineCatalog,
    failed_unit: &str,
    replacement_unit: &str,
    lra: i64,
) -> Result<TimelineCatalog, ChronicleError> {
    let current_ensemble = timeline_catalog
        .segments
        .last()
        .map(|s| s.ensemble.clone())
        .unwrap_or_default();

    let new_ensemble: Vec<String> = current_ensemble
        .iter()
        .map(|u| {
            if u == failed_unit {
                replacement_unit.to_string()
            } else {
                u.clone()
            }
        })
        .collect();

    let new_segment = Segment {
        id: timeline_catalog.segments.len() as i64,
        ensemble: new_ensemble,
        start_offset: lra + 1,
    };

    let mut updated = timeline_catalog.clone();
    updated.segments.push(new_segment);

    catalog
        .put_timeline(&updated, timeline_catalog.version)
        .await
}
