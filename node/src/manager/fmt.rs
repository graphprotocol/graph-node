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
        let minutes = duration.num_minutes();
        if minutes < 90 {
            format!("{}m", duration.num_minutes())
        } else {
            let hours = minutes / 60;
            let minutes = minutes % 60;
            if hours < 24 {
                format!("{}h {}m", hours, minutes)
            } else {
                let days = hours / 24;
                let hours = hours % 24;
                format!("{}d {}h {}m", days, hours, minutes)
            }
        }
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_human_duration() {
        let duration = Duration::seconds(1);
        assert_eq!(human_duration(duration), "1000ms");

        let duration = Duration::seconds(10);
        assert_eq!(human_duration(duration), "10s");

        let duration = Duration::minutes(5);
        assert_eq!(human_duration(duration), "5m");

        let duration = Duration::hours(1);
        assert_eq!(human_duration(duration), "60m");

        let duration = Duration::minutes(100);
        assert_eq!(human_duration(duration), "1h 40m");

        let duration = Duration::days(1);
        assert_eq!(human_duration(duration), "1d 0h 0m");

        let duration = Duration::days(1) + Duration::minutes(35);
        assert_eq!(human_duration(duration), "1d 0h 35m");

        let duration = Duration::days(1) + Duration::minutes(95);
        assert_eq!(human_duration(duration), "1d 1h 35m");
    }
}
