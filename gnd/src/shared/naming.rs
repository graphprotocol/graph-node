//! Alias allocation for overloaded names.
//!
//! Events (and functions) can share a name within one ABI. Both the scaffold
//! (entity names) and the codegen (class names) disambiguate them by suffixing
//! repeats and must agree, so they share this helper.

use std::collections::HashSet;

/// Assign a unique alias to each name, in order. The first use of a name keeps
/// it; repeats get the lowest free numeric suffix (`name1`, `name2`, ...) that
/// isn't a real name in the list or an alias already handed out. So `Transfer`,
/// `Transfer`, `Transfer1` becomes `Transfer`, `Transfer2`, `Transfer1` rather
/// than two `Transfer1`s.
pub fn disambiguate_names(names: &[String]) -> Vec<String> {
    let reserved: HashSet<&str> = names.iter().map(String::as_str).collect();
    let mut used: HashSet<String> = HashSet::new();
    names
        .iter()
        .map(|name| {
            // First occurrence keeps the name; a real name is never taken as a
            // suffix (the loop below skips `reserved`), so it's free here.
            if used.insert(name.clone()) {
                return name.clone();
            }
            let mut n = 1;
            loop {
                let candidate = format!("{name}{n}");
                n += 1;
                if !reserved.contains(candidate.as_str()) && used.insert(candidate.clone()) {
                    return candidate;
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn suffixes_repeats() {
        assert_eq!(
            disambiguate_names(&names(&["Transfer", "Transfer"])),
            names(&["Transfer", "Transfer1"])
        );
    }

    #[test]
    fn suffix_skips_a_real_name() {
        // The second `Transfer` must not take `Transfer1`, which is a real event.
        assert_eq!(
            disambiguate_names(&names(&["Transfer", "Transfer", "Transfer1"])),
            names(&["Transfer", "Transfer2", "Transfer1"])
        );
    }

    #[test]
    fn distinct_names_unchanged() {
        assert_eq!(
            disambiguate_names(&names(&["Approval", "Transfer"])),
            names(&["Approval", "Transfer"])
        );
    }
}
