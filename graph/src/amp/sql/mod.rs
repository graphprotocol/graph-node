pub mod query_builder;

pub use self::query_builder::{BlockRangeQueryBuilder, ContextQuery, ValidQuery};

use anyhow::anyhow;
use lazy_regex::regex_is_match;

/// Validates that `s` is a simple SQL identifier: starts with a letter or
/// underscore and contains only `[a-zA-Z0-9_-]`, up to 101 characters.
pub fn validate_ident(s: &str) -> Result<(), anyhow::Error> {
    if !regex_is_match!("^[a-zA-Z_][a-zA-Z0-9_-]{0,100}$", s) {
        return Err(anyhow!(
            "invalid identifier '{s}': must start with a letter or an underscore, \
             and contain only letters, numbers, hyphens, and underscores"
        ));
    }
    Ok(())
}

/// Normalizes a SQL identifier for safe interpolation into SQL `FROM` clauses.
///
/// Simple identifiers (matching `validate_ident`) are lowercased and returned
/// unquoted. Identifiers with special characters are double-quoted using
/// `sqlparser_latest::ast::Ident::with_quote`.
pub fn normalize_sql_ident(s: &str) -> String {
    match validate_ident(s) {
        Ok(()) => s.to_lowercase(),
        Err(_) => sqlparser_latest::ast::Ident::with_quote('"', s).to_string(),
    }
}
