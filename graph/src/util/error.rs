// Converts back and forth between `failure::Error` and `anyhow::Error`
// while we don't migrate fully to `anyhow`.
pub trait CompatErr {
    type Other;
    fn compat_err(self) -> Self::Other;
}

impl CompatErr for failure::Error {
    type Other = anyhow::Error;

    fn compat_err(self) -> anyhow::Error {
        anyhow::Error::from(self.compat())
    }
}

impl CompatErr for anyhow::Error {
    type Other = failure::Error;

    fn compat_err(self) -> failure::Error {
        // Convert as a single error containing all the causes.
        failure::err_msg(format!("{:#}", self))
    }
}

impl<T, E: CompatErr> CompatErr for Result<T, E> {
    type Other = Result<T, E::Other>;

    fn compat_err(self) -> Self::Other {
        self.map_err(CompatErr::compat_err)
    }
}

// `ensure!` from `anyhow`, but calling `from`.
#[macro_export]
macro_rules! ensure {
    ($cond:expr, $msg:literal $(,)?) => {
        if !$cond {
            return Err(From::from($crate::prelude::anyhow::anyhow!($msg)));
        }
    };
    ($cond:expr, $err:expr $(,)?) => {
        if !$cond {
            return Err(From::from($crate::prelude::anyhow::anyhow!($err)));
        }
    };
    ($cond:expr, $fmt:expr, $($arg:tt)*) => {
        if !$cond {
            return Err(From::from($crate::prelude::anyhow::anyhow!($fmt, $($arg)*)));
        }
    };
}
