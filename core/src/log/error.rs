use std::fmt::Display;
use std::result::Result as StdResult;

use graph::slog::*;

/// An slog `Drain` that logs errors from one `drain` to an `error_drain`.
pub struct WithErrorDrain<D1, D2> {
    /// Name of the first drain (for context).
    name: String,
    /// The drain to catch errors from.
    drain: D1,
    /// The drain to log errors from the first drain to.
    error_drain: D2,
}

impl<D1, D2> WithErrorDrain<D1, D2>
where
    D1: Drain,
    D2: Drain,
{
    /// Creates a new wrapper drain for `drain` that logs errors to `error_drain`.
    pub fn new(name: &str, drain: D1, error_drain: D2) -> Self {
        WithErrorDrain {
            name: name.into(),
            drain,
            error_drain,
        }
    }
}

impl<D1, D2> Drain for WithErrorDrain<D1, D2>
where
    D1: Drain,
    D2: Drain,
    D1::Err: Display,
{
    type Ok = ();
    type Err = D2::Err;

    fn log(&self, record: &Record, values: &OwnedKVList) -> StdResult<Self::Ok, Self::Err> {
        // Attempt to log to `drain`; if that fails, log the error to `error_drain`.
        self.drain.log(record, values).map(|_| ()).or_else(|e| {
            self.error_drain
                .log(
                    &record!(
                        Level::Error,
                        "",
                        &format_args!("Failed to log to {}: {}", self.name, e),
                        b!(),
                    ),
                    &OwnedKVList::from(o!()),
                ).map(|_| ())
        })
    }
}
