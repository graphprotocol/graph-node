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
