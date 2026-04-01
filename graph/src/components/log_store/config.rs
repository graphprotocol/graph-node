use slog::{warn, Logger};
use std::env;
use std::str::FromStr;

/// Read environment variable with fallback to deprecated key
///
/// This helper function implements backward compatibility for environment variables.
/// It first tries the new key, then falls back to the old (deprecated) key with a warning.
///
/// # Arguments
/// * `logger` - Logger for emitting deprecation warnings
/// * `new_key` - The new environment variable name
/// * `old_key` - The deprecated environment variable name
///
/// # Returns
/// The value of the environment variable if found, or None if neither key is set
pub fn read_env_with_fallback(logger: &Logger, new_key: &str, old_key: &str) -> Option<String> {
    // Try new key first
    if let Ok(value) = env::var(new_key) {
        return Some(value);
    }

    // Fall back to old key with deprecation warning
    if let Ok(value) = env::var(old_key) {
        warn!(
            logger,
            "Using deprecated environment variable '{}', please use '{}' instead", old_key, new_key
        );
        return Some(value);
    }

    None
}

/// Read environment variable with default value and fallback
///
/// Similar to `read_env_with_fallback`, but returns a default value if neither key is set.
///
/// # Arguments
/// * `logger` - Logger for emitting deprecation warnings
/// * `new_key` - The new environment variable name
/// * `old_key` - The deprecated environment variable name
/// * `default` - Default value to return if neither key is set
///
/// # Returns
/// The value of the environment variable, or the default if neither key is set
pub fn read_env_with_default(
    logger: &Logger,
    new_key: &str,
    old_key: &str,
    default: &str,
) -> String {
    read_env_with_fallback(logger, new_key, old_key).unwrap_or_else(|| default.to_string())
}

/// Parse a value from environment variable with fallback
///
/// Reads an environment variable with fallback support and parses it using `FromStr`.
/// Returns the default value if the variable is not set or cannot be parsed.
pub fn read_parsed_with_fallback<T: FromStr>(
    logger: &Logger,
    new_key: &str,
    old_key: &str,
    default: T,
) -> T {
    read_env_with_fallback(logger, new_key, old_key)
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_new_key_takes_precedence() {
        let logger = crate::log::logger(true);
        std::env::set_var("NEW_KEY_PRECEDENCE", "new_value");
        std::env::set_var("OLD_KEY_PRECEDENCE", "old_value");

        let result = read_env_with_fallback(&logger, "NEW_KEY_PRECEDENCE", "OLD_KEY_PRECEDENCE");
        assert_eq!(result, Some("new_value".to_string()));

        std::env::remove_var("NEW_KEY_PRECEDENCE");
        std::env::remove_var("OLD_KEY_PRECEDENCE");
    }

    #[test]
    fn test_read_old_key_when_new_not_present() {
        let logger = crate::log::logger(true);
        std::env::remove_var("NEW_KEY_FALLBACK");
        std::env::set_var("OLD_KEY_FALLBACK", "old_value");

        let result = read_env_with_fallback(&logger, "NEW_KEY_FALLBACK", "OLD_KEY_FALLBACK");
        assert_eq!(result, Some("old_value".to_string()));

        std::env::remove_var("OLD_KEY_FALLBACK");
    }

    #[test]
    fn test_read_returns_none_when_neither_present() {
        let logger = crate::log::logger(true);
        std::env::remove_var("NEW_KEY_NONE");
        std::env::remove_var("OLD_KEY_NONE");

        let result = read_env_with_fallback(&logger, "NEW_KEY_NONE", "OLD_KEY_NONE");
        assert_eq!(result, None);
    }

    #[test]
    fn test_read_with_default() {
        let logger = crate::log::logger(true);
        std::env::remove_var("NEW_KEY_DEFAULT");
        std::env::remove_var("OLD_KEY_DEFAULT");

        let result = read_env_with_default(
            &logger,
            "NEW_KEY_DEFAULT",
            "OLD_KEY_DEFAULT",
            "default_value",
        );
        assert_eq!(result, "default_value");

        std::env::remove_var("NEW_KEY_DEFAULT");
        std::env::remove_var("OLD_KEY_DEFAULT");
    }

    #[test]
    fn test_read_parsed_with_fallback() {
        let logger = crate::log::logger(true);

        // Test u64 parsing
        std::env::set_var("NEW_KEY_PARSED", "12345");
        let result: u64 =
            read_parsed_with_fallback(&logger, "NEW_KEY_PARSED", "OLD_KEY_PARSED", 999);
        assert_eq!(result, 12345);
        std::env::remove_var("NEW_KEY_PARSED");

        // Test u32 parsing
        std::env::set_var("NEW_KEY_PARSED", "123");
        let result: u32 =
            read_parsed_with_fallback(&logger, "NEW_KEY_PARSED", "OLD_KEY_PARSED", 999);
        assert_eq!(result, 123);
        std::env::remove_var("NEW_KEY_PARSED");

        // Test with old key fallback
        std::env::set_var("OLD_KEY_PARSED", "67890");
        let result: u64 =
            read_parsed_with_fallback(&logger, "NEW_KEY_PARSED", "OLD_KEY_PARSED", 999);
        assert_eq!(result, 67890);
        std::env::remove_var("OLD_KEY_PARSED");

        // Test with default when neither key is set
        let result: u64 =
            read_parsed_with_fallback(&logger, "NEW_KEY_PARSED", "OLD_KEY_PARSED", 999);
        assert_eq!(result, 999);
    }

    #[test]
    fn test_read_parsed_invalid_uses_default() {
        let logger = crate::log::logger(true);
        std::env::set_var("NEW_KEY_INVALID_PARSED", "not_a_number");

        let result: u64 = read_parsed_with_fallback(
            &logger,
            "NEW_KEY_INVALID_PARSED",
            "OLD_KEY_INVALID_PARSED",
            999,
        );
        assert_eq!(result, 999);

        std::env::remove_var("NEW_KEY_INVALID_PARSED");
    }
}
