use chronicle_proto::pb_catalog::{UnitRegistration, UnitStatus};
use std::collections::HashMap;

fn pressure(u: &UnitRegistration) -> f64 {
    // Weight: CPU and memory are primary, disk is a soft factor
    0.4 * u.cpu_usage + 0.4 * u.memory_usage + 0.2 * u.disk_usage
}

/// Selects ensembles with a three-tier priority:
///
/// 1. **Previous ensemble** — keep members from the prior ensemble if still
///    writable (minimizes data movement on term changes)
/// 2. **Zone diversity** — round-robin across zones so replicas span
///    failure domains
/// 3. **Lowest pressure** — within each zone, pick the unit with the
///    lowest pressure (CPU/memory/disk)
pub struct EnsembleSelector {
    units: Vec<UnitRegistration>,
}

impl EnsembleSelector {
    pub fn new() -> Self {
        Self { units: Vec::new() }
    }

    pub fn from_units(units: Vec<UnitRegistration>) -> Self {
        Self { units }
    }

    /// Replace the full unit list (e.g. after a catalog refresh).
    pub fn update_units(&mut self, units: Vec<UnitRegistration>) {
        self.units = units;
    }

    /// Update load metrics for a single unit.
    pub fn update_metrics(&mut self, address: &str, cpu: f64, memory: f64, disk: f64) {
        if let Some(u) = self.units.iter_mut().find(|u| u.address == address) {
            u.cpu_usage = cpu;
            u.memory_usage = memory;
            u.disk_usage = disk;
        }
    }

    /// Select an ensemble of `rf` units.
    ///
    /// - `previous`: addresses from the prior ensemble — preferred if still writable
    /// - `exclude`: addresses to never select
    pub fn select(
        &self,
        rf: usize,
        previous: &[String],
        exclude: &[String],
    ) -> Option<Vec<String>> {
        let candidates: Vec<&UnitRegistration> = self
            .units
            .iter()
            .filter(|u| u.status() == UnitStatus::Writable && !exclude.contains(&u.address))
            .collect();

        if candidates.len() < rf {
            return None;
        }

        let mut ensemble: Vec<String> = Vec::with_capacity(rf);
        let mut used_zones: HashMap<&str, usize> = HashMap::new();

        // Phase 1: retain previous ensemble members that are still writable
        for addr in previous {
            if ensemble.len() >= rf {
                break;
            }
            if let Some(u) = candidates.iter().find(|u| u.address == *addr) {
                ensemble.push(u.address.clone());
                let zone = if u.zone.is_empty() { "default" } else { u.zone.as_str() };
                *used_zones.entry(zone).or_default() += 1;
            }
        }

        if ensemble.len() >= rf {
            return Some(ensemble);
        }

        // Phase 2: fill remaining slots with zone-diverse, low-pressure units
        let remaining: Vec<&UnitRegistration> = candidates
            .iter()
            .filter(|u| !ensemble.contains(&u.address))
            .copied()
            .collect();

        // Group by zone, sort each group by pressure (ascending)
        let mut by_zone: HashMap<&str, Vec<&UnitRegistration>> = HashMap::new();
        for u in &remaining {
            let zone = if u.zone.is_empty() { "default" } else { u.zone.as_str() };
            by_zone.entry(zone).or_default().push(u);
        }
        for group in by_zone.values_mut() {
            group.sort_by(|a, b| {
                pressure(a)
                    .partial_cmp(&pressure(b))
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }

        // Sort zones: prefer zones with fewer already-selected units
        let mut zone_keys: Vec<&str> = by_zone.keys().copied().collect();
        zone_keys.sort_by(|a, b| {
            let count_a = used_zones.get(a).copied().unwrap_or(0);
            let count_b = used_zones.get(b).copied().unwrap_or(0);
            count_a.cmp(&count_b).then(a.cmp(b))
        });

        let mut zone_cursors = vec![0usize; zone_keys.len()];

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

    fn unit(addr: &str, zone: &str) -> UnitRegistration {
        UnitRegistration {
            address: addr.into(),
            zone: zone.into(),
            status: UnitStatus::Writable as i32,
            cpu_usage: 0.0,
            memory_usage: 0.0,
            disk_usage: 0.0,
        }
    }

    fn unit_with_load(addr: &str, zone: &str, cpu: f64, mem: f64, disk: f64) -> UnitRegistration {
        UnitRegistration {
            address: addr.into(),
            zone: zone.into(),
            status: UnitStatus::Writable as i32,
            cpu_usage: cpu,
            memory_usage: mem,
            disk_usage: disk,
        }
    }

    #[test]
    fn basic_select() {
        let sel = EnsembleSelector::from_units(vec![
            unit("a", "us-east-1"),
            unit("b", "us-west-2"),
            unit("c", "eu-west-1"),
        ]);
        let result = sel.select(2, &[], &[]).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn previous_ensemble_preferred() {
        let sel = EnsembleSelector::from_units(vec![
            unit("a", "zone-a"),
            unit("b", "zone-b"),
            unit("c", "zone-c"),
            unit("d", "zone-d"),
        ]);
        let prev = vec!["b".into(), "c".into()];
        let result = sel.select(3, &prev, &[]).unwrap();
        assert!(result.contains(&"b".to_string()));
        assert!(result.contains(&"c".to_string()));
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn previous_skips_non_writable() {
        let mut b = unit("b", "zone-b");
        b.status = UnitStatus::Readonly as i32;
        let sel = EnsembleSelector::from_units(vec![
            unit("a", "zone-a"),
            b,
            unit("c", "zone-c"),
            unit("d", "zone-d"),
        ]);
        let prev = vec!["b".into(), "c".into()];
        let result = sel.select(2, &prev, &[]).unwrap();
        assert!(!result.contains(&"b".to_string()));
        assert!(result.contains(&"c".to_string()));
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
        let result = sel.select(3, &[], &[]).unwrap();
        let zones: Vec<&str> = result
            .iter()
            .map(|addr| {
                let u = sel.units.iter().find(|u| u.address == *addr).unwrap();
                u.zone.as_str()
            })
            .collect();
        let mut unique = zones.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), 3, "should pick from 3 different zones");
    }

    #[test]
    fn prefers_low_pressure() {
        let sel = EnsembleSelector::from_units(vec![
            unit_with_load("hot", "zone-a", 0.9, 0.8, 0.5),
            unit_with_load("cold", "zone-a", 0.1, 0.1, 0.1),
            unit_with_load("other", "zone-b", 0.2, 0.2, 0.1),
        ]);
        let result = sel.select(2, &[], &[]).unwrap();
        assert!(result.contains(&"cold".to_string()));
        assert!(result.contains(&"other".to_string()));
    }

    #[test]
    fn zone_diversity_after_previous() {
        let sel = EnsembleSelector::from_units(vec![
            unit("a1", "zone-a"),
            unit("a2", "zone-a"),
            unit("a3", "zone-a"),
            unit("b1", "zone-b"),
        ]);
        let prev = vec!["a1".into(), "a2".into()];
        let result = sel.select(3, &prev, &[]).unwrap();
        assert!(result.contains(&"a1".to_string()));
        assert!(result.contains(&"a2".to_string()));
        assert!(result.contains(&"b1".to_string()));
    }

    #[test]
    fn respects_exclude() {
        let sel = EnsembleSelector::from_units(vec![
            unit("a", "zone-a"),
            unit("b", "zone-b"),
            unit("c", "zone-c"),
        ]);
        let result = sel.select(2, &[], &["a".into()]).unwrap();
        assert!(!result.contains(&"a".to_string()));
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn insufficient_units() {
        let sel = EnsembleSelector::from_units(vec![unit("a", "zone-a")]);
        assert!(sel.select(2, &[], &[]).is_none());
    }

    #[test]
    fn skips_non_writable() {
        let mut u = unit("a", "zone-a");
        u.status = UnitStatus::Readonly as i32;
        let sel = EnsembleSelector::from_units(vec![u, unit("b", "zone-b")]);
        assert!(sel.select(2, &[], &[]).is_none());
    }

    #[test]
    fn update_metrics() {
        let mut sel = EnsembleSelector::from_units(vec![
            unit_with_load("a", "zone-a", 0.0, 0.0, 0.0),
            unit_with_load("b", "zone-a", 0.5, 0.5, 0.3),
            unit_with_load("c", "zone-b", 0.3, 0.2, 0.1),
        ]);
        let result = sel.select(2, &[], &[]).unwrap();
        assert!(result.contains(&"a".to_string()));

        // Now a gets heavy load
        sel.update_metrics("a", 0.95, 0.9, 0.8);
        let result = sel.select(2, &[], &[]).unwrap();
        assert!(result.contains(&"b".to_string()));
        assert!(result.contains(&"c".to_string()));
    }

    #[test]
    fn single_zone_fallback() {
        let sel = EnsembleSelector::from_units(vec![
            unit("a", "zone-a"),
            unit("b", "zone-a"),
            unit("c", "zone-a"),
        ]);
        let result = sel.select(3, &[], &[]).unwrap();
        assert_eq!(result.len(), 3);
    }
}
