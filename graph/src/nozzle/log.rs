use std::borrow::Cow;

use lazy_regex::regex_replace_all;

/// Extends the [slog::Logger] with methods commonly used in Nozzle modules
pub trait Logger {
    /// Creates a new child logger scoped to a specific component
    fn component(&self, name: &'static str) -> slog::Logger;
}

impl Logger for slog::Logger {
    fn component(&self, name: &'static str) -> slog::Logger {
        self.new(slog::o!("component" => name))
    }
}

/// Removes newlines and extra spaces from a string
pub fn one_line<'a>(s: &'a str) -> Cow<'a, str> {
    regex_replace_all!(r"(\\r)?(\\n)?\s+", s, " ")
}
