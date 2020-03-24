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

/// Panics if there is no current tokio::Runtime
pub fn block_on_allow_panic<T>(f: impl Future03<Output = T> + Send) -> T {
    let rt = tokio::runtime::Handle::current();

    rt.enter(move || {
        // TODO: It should be possible to just call block_on directly, but we
        // ran into this bug https://github.com/rust-lang/futures-rs/issues/2090
        // which will panic if we're already in block_on. So, detect if this
        // function would panic. Once we fix this, we can remove the requirement
        // of + Send for f (and few functions that call this that had +Send
        // added) The test which ran into this bug was
        // runtime::wasm::test::ipfs_fail
        let enter = futures03::executor::enter();
        let oh_no = enter.is_err();
        drop(enter);

        // If the function would panic, fallback to the old ways.
        if oh_no {
            use futures::future::Future as _;
            let compat = async { Result::<T, ()>::Ok(f.await) }.boxed().compat();
            // This unwrap does not panic because of the above line always returning Ok
            compat.wait().unwrap()
        } else {
            block_on(f)
        }
    })
}
