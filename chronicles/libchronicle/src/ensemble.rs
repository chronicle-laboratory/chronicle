use std::collections::HashMap;

/// Runtime info about a unit, combining static metadata (zone) with
/// dynamic metrics (traffic, pressure).
#[derive(Debug, Clone)]
pub struct UnitInfo {
    pub address: String,
    pub zone: String,
    pub traffic: f64,
    pub pressure: f64,
    pub writable: bool,
}

impl UnitInfo {
    fn score(&self) -> f64 {
        self.traffic + self.pressure * 2.0
    }
}

/// Selects ensembles using zone-aware placement, traffic load, and backpressure.
///
/// Selection strategy:
/// 1. Filter to writable, non-excluded candidates
/// 2. Sort candidates within each zone by score (traffic + 2×pressure)
/// 3. Round-robin across zones to maximize zone diversity
/// 4. Within each zone pick the lowest-scored unit first
pub struct EnsembleSelector {
    units: Vec<UnitInfo>,
}

impl EnsembleSelector {
    pub fn new() -> Self {
        Self { units: Vec::new() }
    }

    pub fn from_units(units: Vec<UnitInfo>) -> Self {
        Self { units }
    }

    /// Replace the full unit list (e.g. after a catalog refresh).
    pub fn update_units(&mut self, units: Vec<UnitInfo>) {
        self.units = units;
    }

    /// Update traffic and pressure for a single unit.
    pub fn update_metrics(&mut self, address: &str, traffic: f64, pressure: f64) {
        if let Some(u) = self.units.iter_mut().find(|u| u.address == address) {
            u.traffic = traffic;
            u.pressure = pressure;
        }
    }

    /// Select an ensemble of `rf` units, excluding the given addresses.
    pub fn select(&self, rf: usize, exclude: &[String]) -> Option<Vec<String>> {
        let candidates: Vec<&UnitInfo> = self
            .units
            .iter()
            .filter(|u| u.writable && !exclude.contains(&u.address))
            .collect();

        if candidates.len() < rf {
            return None;
        }

        // Group by zone, sort each group by score (lower = better)
        let mut by_zone: HashMap<&str, Vec<&UnitInfo>> = HashMap::new();
        for u in &candidates {
            by_zone.entry(u.zone.as_str()).or_default().push(u);
        }
        for group in by_zone.values_mut() {
            group.sort_by(|a, b| {
                a.score()
                    .partial_cmp(&b.score())
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }

        // Round-robin across zones to spread replicas
        let mut zone_keys: Vec<&str> = by_zone.keys().copied().collect();
        zone_keys.sort();

        let mut zone_cursors = vec![0usize; zone_keys.len()];
        let mut ensemble = Vec::with_capacity(rf);

        let mut passes = 0;
        while ensemble.len() < rf {
            let mut added_this_pass = false;
            for (zi, zone) in zone_keys.iter().enumerate() {
                if ensemble.len() >= rf {
                    break;
                }
                let group = &by_zone[zone];
                if zone_cursors[zi] < group.len() {
                    ensemble.push(group[zone_cursors[zi]].address.clone());
                    zone_cursors[zi] += 1;
                    added_this_pass = true;
                }
            }
            if !added_this_pass {
                break;
            }
            passes += 1;
            if passes > rf {
                break;
            }
        }

        if ensemble.len() >= rf {
            Some(ensemble)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unit(addr: &str, zone: &str) -> UnitInfo {
        UnitInfo {
            address: addr.into(),
            zone: zone.into(),
            traffic: 0.0,
            pressure: 0.0,
            writable: true,
        }
    }

    fn unit_with_metrics(addr: &str, zone: &str, traffic: f64, pressure: f64) -> UnitInfo {
        UnitInfo {
            address: addr.into(),
            zone: zone.into(),
            traffic,
            pressure,
            writable: true,
        }
    }

    #[test]
    fn basic_select() {
        let sel = EnsembleSelector::from_units(vec![
            unit("a", "us-east-1"),
            unit("b", "us-west-2"),
            unit("c", "eu-west-1"),
        ]);
        let result = sel.select(2, &[]).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn zone_spread() {
        let sel = EnsembleSelector::from_units(vec![
            unit("a1", "zone-a"),
            unit("a2", "zone-a"),
            unit("b1", "zone-b"),
            unit("b2", "zone-b"),
            unit("c1", "zone-c"),
        ]);
        let result = sel.select(3, &[]).unwrap();
        // Should pick one from each zone
        let zones: Vec<&str> = result
            .iter()
            .map(|addr| {
                sel.units
                    .iter()
                    .find(|u| u.address == *addr)
                    .unwrap()
                    .zone
                    .as_str()
            })
            .collect();
        assert_eq!(zones.len(), 3);
        // All zones should be unique
        let mut unique = zones.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), 3);
    }

    #[test]
    fn prefers_low_traffic() {
        let sel = EnsembleSelector::from_units(vec![
            unit_with_metrics("hot", "zone-a", 0.9, 0.0),
            unit_with_metrics("cold", "zone-a", 0.1, 0.0),
            unit_with_metrics("other", "zone-b", 0.5, 0.0),
        ]);
        let result = sel.select(2, &[]).unwrap();
        // Should pick cold (zone-a, low traffic) and other (zone-b)
        assert!(result.contains(&"cold".to_string()));
        assert!(result.contains(&"other".to_string()));
    }

    #[test]
    fn avoids_high_pressure() {
        let sel = EnsembleSelector::from_units(vec![
            unit_with_metrics("pressured", "zone-a", 0.1, 0.9),
            unit_with_metrics("healthy", "zone-a", 0.3, 0.0),
            unit_with_metrics("other", "zone-b", 0.2, 0.0),
        ]);
        let result = sel.select(2, &[]).unwrap();
        assert!(result.contains(&"healthy".to_string()));
        assert!(result.contains(&"other".to_string()));
    }

    #[test]
    fn respects_exclude() {
        let sel = EnsembleSelector::from_units(vec![
            unit("a", "zone-a"),
            unit("b", "zone-b"),
            unit("c", "zone-c"),
        ]);
        let result = sel.select(2, &["a".into()]).unwrap();
        assert!(!result.contains(&"a".to_string()));
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn insufficient_units() {
        let sel = EnsembleSelector::from_units(vec![unit("a", "zone-a")]);
        assert!(sel.select(2, &[]).is_none());
    }

    #[test]
    fn skips_non_writable() {
        let mut u = unit("a", "zone-a");
        u.writable = false;
        let sel = EnsembleSelector::from_units(vec![u, unit("b", "zone-b")]);
        assert!(sel.select(2, &[]).is_none());
    }

    #[test]
    fn update_metrics() {
        let mut sel = EnsembleSelector::from_units(vec![
            unit_with_metrics("a", "zone-a", 0.1, 0.0),
            unit_with_metrics("b", "zone-a", 0.5, 0.0),
            unit_with_metrics("c", "zone-b", 0.3, 0.0),
        ]);
        // a is preferred initially
        let result = sel.select(2, &[]).unwrap();
        assert!(result.contains(&"a".to_string()));

        // Now a gets heavy pressure
        sel.update_metrics("a", 0.1, 1.0);
        let result = sel.select(2, &[]).unwrap();
        // b should be picked from zone-a now
        assert!(result.contains(&"b".to_string()));
        assert!(result.contains(&"c".to_string()));
    }

    #[test]
    fn single_zone_fallback() {
        // All units in same zone — should still work
        let sel = EnsembleSelector::from_units(vec![
            unit("a", "zone-a"),
            unit("b", "zone-a"),
            unit("c", "zone-a"),
        ]);
        let result = sel.select(3, &[]).unwrap();
        assert_eq!(result.len(), 3);
    }
}
