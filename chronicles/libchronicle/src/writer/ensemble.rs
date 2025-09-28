/// Select an ensemble of units for a timeline segment (TLA+ `FindEnsemble`).
///
/// Starts with `include` units, then fills remaining slots from `available`
/// (excluding any in `exclude`). Returns `None` if fewer than `rf` units
/// can be assembled.
pub fn select_ensemble(
    available: &[String],
    include: &[String],
    exclude: &[String],
    rf: usize,
) -> Option<Vec<String>> {
    let mut ensemble: Vec<String> = include.to_vec();

    for unit in available {
        if ensemble.len() >= rf {
            break;
        }
        if !ensemble.contains(unit) && !exclude.contains(unit) {
            ensemble.push(unit.clone());
        }
    }

    if ensemble.len() >= rf {
        Some(ensemble)
    } else {
        None
    }
}

/// Check whether a valid ensemble exists (TLA+ `HasTargetEnsemble`).
pub fn has_valid_ensemble(
    available: &[String],
    include: &[String],
    exclude: &[String],
    rf: usize,
) -> bool {
    select_ensemble(available, include, exclude, rf).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_ensemble_basic() {
        let available = vec!["a".into(), "b".into(), "c".into()];
        let result = select_ensemble(&available, &[], &[], 2).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn select_ensemble_with_include() {
        let available = vec!["a".into(), "b".into(), "c".into()];
        let include = vec!["b".into()];
        let result = select_ensemble(&available, &include, &[], 2).unwrap();
        assert!(result.contains(&"b".to_string()));
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn select_ensemble_with_exclude() {
        let available = vec!["a".into(), "b".into(), "c".into()];
        let exclude = vec!["a".into(), "b".into()];
        let result = select_ensemble(&available, &[], &exclude, 2);
        assert!(result.is_none());
    }

    #[test]
    fn select_ensemble_insufficient() {
        let available = vec!["a".into()];
        assert!(select_ensemble(&available, &[], &[], 2).is_none());
    }
}
