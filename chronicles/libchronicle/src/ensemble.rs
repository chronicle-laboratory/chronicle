pub(crate) fn select(
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        let available = vec!["a".into(), "b".into(), "c".into()];
        let result = select(&available, &[], &[], 2).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn with_include() {
        let available = vec!["a".into(), "b".into(), "c".into()];
        let include = vec!["b".into()];
        let result = select(&available, &include, &[], 2).unwrap();
        assert!(result.contains(&"b".to_string()));
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn with_exclude() {
        let available = vec!["a".into(), "b".into(), "c".into()];
        let exclude = vec!["a".into(), "b".into()];
        let result = select(&available, &[], &exclude, 2);
        assert!(result.is_none());
    }

    #[test]
    fn insufficient() {
        let available = vec!["a".into()];
        assert!(select(&available, &[], &[], 2).is_none());
    }
}
