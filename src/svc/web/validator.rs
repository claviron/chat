use regex::RegexSet;
use tracing::error;

/// Compiled origin patterns (exact + regex)
pub struct OriginValidator {
    pub(crate) exact: Vec<String>,
    pub(crate) patterns: Option<RegexSet>,
}

impl OriginValidator {
    pub fn new(patterns: &[String]) -> Self {
        let mut exact = Vec::new();
        let mut regex_patterns = Vec::new();

        for pattern in patterns {
            // If looks like regex (has ^, *, \, etc.), treat as regex
            if pattern.contains('^')
                || pattern.contains('*')
                || pattern.contains('\\')
            {
                regex_patterns.push(pattern.clone());
            } else {
                exact.push(pattern.clone());
            }
        }

        let compiled = if regex_patterns.is_empty() {
            None
        } else {
            match RegexSet::new(&regex_patterns) {
                Ok(set) => Some(set),
                Err(e) => {
                    error!("Failed to compile origin regex patterns: {}", e);
                    None
                }
            }
        };

        Self { exact, patterns: compiled }
    }

    pub fn is_allowed(
        &self,
        origin_value: &str,
    ) -> bool {
        self.exact.iter().any(|exact| exact == origin_value)
            || self
                .patterns
                .as_ref()
                // .map_or(false, |set| set.is_match(origin_value))
                .is_some_and(|set| set.is_match(origin_value))
    }
}
