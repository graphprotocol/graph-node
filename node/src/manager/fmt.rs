use std::time::SystemTime;

use graph::prelude::chrono::{DateTime, Duration, Local, Utc};

pub const NULL: &str = "ø";
const CHECK: &str = "✓";

pub fn null() -> String {
    NULL.to_string()
}

pub fn check() -> String {
    CHECK.to_string()
}

pub trait MapOrNull<T> {
    fn map_or_null<F>(&self, f: F) -> String
    where
        F: FnOnce(&T) -> String;
}

impl<T> MapOrNull<T> for Option<T> {
    fn map_or_null<F>(&self, f: F) -> String
    where
        F: FnOnce(&T) -> String,
    {
        self.as_ref()
            .map(|value| f(value))
            .unwrap_or_else(|| NULL.to_string())
    }
}

/// Return the duration from `start` to `end` formatted using
/// `human_duration`. Use now if `end` is `None`
pub fn duration(start: &DateTime<Utc>, end: &Option<DateTime<Utc>>) -> String {
    let start = *start;
    let end = *end;

    let end = end.unwrap_or(DateTime::<Utc>::from(SystemTime::now()));
    let duration = end - start;

    human_duration(duration)
}

/// Format a duration using ms/s/m as units depending on how long the
/// duration was
pub fn human_duration(duration: Duration) -> String {
    if duration.num_seconds() < 5 {
        format!("{}ms", duration.num_milliseconds())
    } else if duration.num_minutes() < 5 {
        format!("{}s", duration.num_seconds())
    } else {
        format!("{}m", duration.num_minutes())
    }
}

/// Abbreviate a long name to fit into `size` characters. The abbreviation
/// is done by replacing the middle of the name with `..`. For example, if
/// `name` is `foo_bar_baz` and `size` is 10, the result will be
/// `foo.._baz`. If the name is shorter than `size`, it is returned
/// unchanged.
pub fn abbreviate(name: &str, size: usize) -> String {
    if name.len() > size {
        let fragment = size / 2 - 2;
        let last = name.len() - fragment;
        let mut name = name.to_string();
        name.replace_range(fragment..last, "..");
        let table = name.trim().to_string();
        table
    } else {
        name.to_string()
    }
}

pub fn date_time(date: &DateTime<Utc>) -> String {
    let date = DateTime::<Local>::from(*date);
    date.format("%Y-%m-%d %H:%M:%S%Z").to_string()
}
