use futures03::executor::block_on;
use futures03::future::{FutureExt, TryFutureExt};
use std::future::Future as Future03;
use std::panic::AssertUnwindSafe;
use tokio::task::JoinHandle;

fn abort_on_panic<T: Send + 'static>(
    f: impl Future03<Output = T> + Send + 'static,
) -> impl Future03<Output = T> {
    // We're crashing, unwind safety doesn't matter.
    AssertUnwindSafe(f).catch_unwind().unwrap_or_else(|_| {
        println!("Panic in tokio task, aborting!");
        std::process::abort()
    })
}

/// Aborts on panic.
pub fn spawn<T: Send + 'static>(f: impl Future03<Output = T> + Send + 'static) -> JoinHandle<T> {
    tokio::spawn(abort_on_panic(f))
}

pub fn spawn_allow_panic<T: Send + 'static>(
    f: impl Future03<Output = T> + Send + 'static,
) -> JoinHandle<T> {
    tokio::spawn(f)
}

/// Aborts on panic.
pub fn spawn_blocking<T: Send + 'static>(
    f: impl Future03<Output = T> + Send + 'static,
) -> JoinHandle<T> {
    tokio::task::spawn_blocking(move || block_on(abort_on_panic(f)))
}

/// Panics result in an `Err` in `JoinHandle`.
pub fn spawn_blocking_allow_panic<T: Send + 'static>(
    f: impl Future03<Output = T> + Send + 'static,
) -> JoinHandle<T> {
    tokio::task::spawn_blocking(move || block_on(f))
}

/// Does not abort on panic
pub async fn spawn_blocking_async_allow_panic<R: 'static + Send>(
    f: impl 'static + FnOnce() -> R + Send,
) -> R {
    tokio::task::spawn_blocking(f).await.unwrap()
}

pub fn block_on_allow_panic<T>(f: impl Future03<Output = T>) -> T {
    block_on(f)
}
