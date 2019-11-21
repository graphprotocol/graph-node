use failure::Error;
use futures::sync::oneshot;
use std::fmt;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
use tokio::prelude::{future::Fuse, Async, Future, Poll, Stream};

/// A cancelable stream or future.
///
/// Created by calling `cancelable` extension method.
/// Can be canceled through the corresponding `CancelGuard`.
pub struct Cancelable<T, C> {
    inner: T,
    cancel_receiver: Fuse<oneshot::Receiver<()>>,
    on_cancel: C,
}

/// It's not viable to use `select` directly, so we do a custom implementation.
impl<S: Stream, C: Fn() -> S::Error> Stream for Cancelable<S, C> {
    type Item = S::Item;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // Error if the stream was canceled by dropping the sender.
        // `cancel_receiver` is fused so we may ignore `Ok`s.
        if self.cancel_receiver.poll().is_err() {
            Err((self.on_cancel)())
        // Otherwise poll it.
        } else {
            self.inner.poll()
        }
    }
}

impl<F: Future, C: Fn() -> F::Error> Future for Cancelable<F, C> {
    type Item = F::Item;
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // Error if the future was canceled by dropping the sender.
        // `cancel_receiver` is fused so we may ignore `Ok`s.
        if self.cancel_receiver.poll().is_err() {
            Err((self.on_cancel)())
        // Otherwise poll it.
        } else {
            self.inner.poll()
        }
    }
}

/// A `CancelGuard` or `SharedCancelGuard`.
pub trait Canceler {
    /// Adds `cancel_sender` to the set being guarded.
    /// Avoid calling directly and prefer using `cancelable`.
    fn add_cancel_sender(&self, cancel_sender: oneshot::Sender<()>);
}

/// Cancels any guarded futures and streams when dropped.
#[derive(Debug, Default)]
pub struct CancelGuard {
    /// This is the only non-temporary strong reference to this `Arc`, therefore
    /// the `Vec` should be dropped shortly after `self` is dropped.
    cancel_senders: Arc<Mutex<Vec<oneshot::Sender<()>>>>,
}

impl CancelGuard {
    /// Creates a guard that initially guards nothing.
    pub fn new() -> Self {
        Self::default()
    }

    /// A more readable `drop`.
    pub fn cancel(self) {}

    pub fn handle(&self) -> CancelHandle {
        CancelHandle {
            guard: Arc::downgrade(&self.cancel_senders),
        }
    }
}

impl Canceler for CancelGuard {
    fn add_cancel_sender(&self, cancel_sender: oneshot::Sender<()>) {
        self.cancel_senders.lock().unwrap().push(cancel_sender);
    }
}

/// A shared handle to a guard, used to add more cancelables. The handle
/// may outlive the guard, if `cancelable` is called with a handle to a
/// dropped guard, then the future or stream it is immediately canceled.
///
/// Dropping a handle has no effect.
#[derive(Clone, Debug)]
pub struct CancelHandle {
    guard: Weak<Mutex<Vec<oneshot::Sender<()>>>>,
}

impl CancelHandle {
    pub fn is_canceled(&self) -> bool {
        // Has been canceled if and only if the guard is gone.
        self.guard.upgrade().is_none()
    }
}

impl Canceler for CancelHandle {
    fn add_cancel_sender(&self, cancel_sender: oneshot::Sender<()>) {
        if let Some(guard) = self.guard.upgrade() {
            // If the guard exists, register the canceler.
            guard.lock().unwrap().push(cancel_sender);
        } else {
            // Otherwise cancel immediately.
            drop(cancel_sender)
        }
    }
}

/// A cancelation guard that can be canceled through a shared reference such as
/// an `Arc`.
///
/// To cancel guarded streams or futures, call `cancel` or drop the guard.
#[derive(Debug, Default)]
pub struct SharedCancelGuard {
    guard: Mutex<Option<CancelGuard>>,
}

impl SharedCancelGuard {
    /// Creates a guard that initially guards nothing.
    pub fn new() -> Self {
        Self::default()
    }

    /// Cancels the stream, a noop if already canceled.
    pub fn cancel(&self) {
        *self.guard.lock().unwrap() = None
    }

    pub fn is_canceled(&self) -> bool {
        self.guard.lock().unwrap().is_none()
    }

    pub fn handle(&self) -> CancelHandle {
        if let Some(ref guard) = *self.guard.lock().unwrap() {
            guard.handle()
        } else {
            // A handle that is always canceled.
            CancelHandle { guard: Weak::new() }
        }
    }
}

impl Canceler for SharedCancelGuard {
    /// Cancels immediately if `self` has already been canceled.
    fn add_cancel_sender(&self, cancel_sender: oneshot::Sender<()>) {
        if let Some(ref mut guard) = *self.guard.lock().unwrap() {
            guard.add_cancel_sender(cancel_sender);
        } else {
            drop(cancel_sender)
        }
    }
}

/// An implementor of `Canceler` that never cancels,
/// making `cancelable` a noop.
#[derive(Debug, Default)]
pub struct DummyCancelGuard;

impl Canceler for DummyCancelGuard {
    fn add_cancel_sender(&self, cancel_sender: oneshot::Sender<()>) {
        // Send to the channel, preventing cancelation.
        let _ = cancel_sender.send(());
    }
}

pub trait StreamExtension: Stream + Sized {
    /// When `cancel` is called on a `CancelGuard` or it is dropped,
    /// `Cancelable` receives an error.
    ///
    /// `on_cancel` is called to make an error value upon cancelation.
    fn cancelable<C: Fn() -> Self::Error>(
        self,
        guard: &impl Canceler,
        on_cancel: C,
    ) -> Cancelable<Self, C>;
}

impl<S: Stream> StreamExtension for S {
    fn cancelable<C: Fn() -> S::Error>(
        self,
        guard: &impl Canceler,
        on_cancel: C,
    ) -> Cancelable<Self, C> {
        let (canceler, cancel_receiver) = oneshot::channel();
        guard.add_cancel_sender(canceler);
        Cancelable {
            inner: self,
            cancel_receiver: cancel_receiver.fuse(),
            on_cancel,
        }
    }
}

pub trait FutureExtension: Future + Sized {
    /// When `cancel` is called on a `CancelGuard` or it is dropped,
    /// `Cancelable` receives an error.
    ///
    /// `on_cancel` is called to make an error value upon cancelation.
    fn cancelable<C: Fn() -> Self::Error>(
        self,
        guard: &impl Canceler,
        on_cancel: C,
    ) -> Cancelable<Self, C>;

    /// Measures the time it takes for the future to complete.
    fn measure<C: FnOnce(&Self::Item, Duration)>(self, callback: C) -> Measure<Self, C>;
}

impl<F: Future> FutureExtension for F {
    fn cancelable<C: Fn() -> F::Error>(
        self,
        guard: &impl Canceler,
        on_cancel: C,
    ) -> Cancelable<Self, C> {
        let (canceler, cancel_receiver) = oneshot::channel();
        guard.add_cancel_sender(canceler);
        Cancelable {
            inner: self,
            cancel_receiver: cancel_receiver.fuse(),
            on_cancel,
        }
    }

    fn measure<C: FnOnce(&Self::Item, Duration)>(self, callback: C) -> Measure<Self, C> {
        Measure::new(self, callback)
    }
}

#[derive(Debug)]
pub enum CancelableError<E> {
    Cancel,
    Error(E),
}

impl From<Error> for CancelableError<Error> {
    fn from(e: Error) -> Self {
        CancelableError::Error(e)
    }
}

impl<E> fmt::Display for CancelableError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CancelableError::Error(e) => e.fmt(f),
            CancelableError::Cancel => write!(f, "operation canceled"),
        }
    }
}

/// Wraps a future and measures the time from creation to completion.
///
/// Example usage:
/// ```
/// extern crate graph;
/// use graph::prelude::*;
///
/// ...
/// some_future
///   .measure(|duration| println!("Duration: {:?}", duration))
/// ```
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub struct Measure<F, C> {
    inner: F,
    callback: Option<C>,
    start: Instant,
}

impl<F, C> Measure<F, C> {
    fn new(inner: F, callback: C) -> Self {
        Self {
            inner,
            callback: Some(callback),
            start: Instant::now(),
        }
    }
}

impl<F, C> Future for Measure<F, C>
where
    F: Future,
    C: FnOnce(&F::Item, Duration),
{
    type Item = F::Item;
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.inner.poll().map(|v| match v {
            Async::Ready(v) => {
                (self.callback.take().expect("cannot poll Measure twice"))(
                    &v,
                    self.start.elapsed(),
                );
                Async::Ready(v)
            }
            Async::NotReady => Async::NotReady,
        })
    }
}
