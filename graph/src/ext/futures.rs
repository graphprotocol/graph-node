use futures::sync::oneshot;
use std::sync::{Arc, Mutex, Weak};
use tokio::prelude::{Future, Poll, Stream};

/// A cancelable stream or future.
///
/// It can be canceled through the corresponding `CancelGuard`.
pub struct Cancelable<T, C> {
    cancelable: T,
    canceled: oneshot::Receiver<()>,
    on_cancel: C,
}

/// It's not viable to use `select` directly, so we do a custom implementation.
impl<S: Stream, C: Fn() -> S::Error> Stream for Cancelable<S, C> {
    type Item = S::Item;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // Error if the stream was canceled by dropping the sender.
        if self.canceled.poll().is_err() {
            Err((self.on_cancel)())
        // Otherwise poll it.
        } else {
            self.cancelable.poll()
        }
    }
}

impl<F: Future, C: Fn() -> F::Error> Future for Cancelable<F, C> {
    type Item = F::Item;
    type Error = F::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // Error if the future was canceled by dropping the sender.
        if self.canceled.poll().is_err() {
            Err((self.on_cancel)())
        // Otherwise poll it.
        } else {
            self.cancelable.poll()
        }
    }
}

/// A `CancelGuard` or `SharedCancelGuard`.
pub trait CancelGuardTrait {
    /// Adds `canceler` to the set being guarded.
    fn add_canceler(&self, canceler: oneshot::Sender<()>);
}

/// Cancels any guarded futures and streams when dropped.
#[derive(Debug, Default)]
pub struct CancelGuard {
    /// This is the only non-temporary strong reference to this `Arc`, therefore
    /// the `Vec` should be dropped shortly after `self` is dropped.
    cancelers: Arc<Mutex<Vec<oneshot::Sender<()>>>>,
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
            guard: Arc::downgrade(&self.cancelers),
        }
    }
}

impl CancelGuardTrait for CancelGuard {
    fn add_canceler(&self, canceler: oneshot::Sender<()>) {
        self.cancelers.lock().unwrap().push(canceler);
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

impl CancelGuardTrait for CancelHandle {
    fn add_canceler(&self, canceler: oneshot::Sender<()>) {
        if let Some(guard) = self.guard.upgrade() {
            // If the guard exists, register the canceler.
            guard.lock().unwrap().push(canceler);
        } else {
            // Otherwise cancel immediately.
            drop(canceler)
        }
    }
}

/// A version of `CancelGuard` that can be canceled through a shared reference.
///
/// To cancel guarded streams or futures, call `cancel` on one of the guards or
/// drop all guards.
#[derive(Clone, Debug, Default)]
pub struct SharedCancelGuard {
    guard: Arc<Mutex<Option<CancelGuard>>>,
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

impl CancelGuardTrait for SharedCancelGuard {
    /// A noop if `self` has already been canceled.
    fn add_canceler(&self, canceler: oneshot::Sender<()>) {
        if let Some(ref mut guard) = *self.guard.lock().unwrap() {
            guard.add_canceler(canceler);
        }
    }
}

pub trait StreamExtension: Stream + Sized {
    /// When `cancel` is called on a `CancelGuard` or it is dropped,
    /// `Cancelable` receives an error.
    ///
    /// `on_cancel` is called to make an error value upon cancelation.
    fn cancelable<C: Fn() -> Self::Error>(
        self,
        guard: &impl CancelGuardTrait,
        on_cancel: C,
    ) -> Cancelable<Self, C>;
}

impl<S: Stream> StreamExtension for S {
    fn cancelable<C: Fn() -> S::Error>(
        self,
        guard: &impl CancelGuardTrait,
        on_cancel: C,
    ) -> Cancelable<Self, C> {
        let (canceler, canceled) = oneshot::channel();
        guard.add_canceler(canceler);
        Cancelable {
            cancelable: self,
            canceled,
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
        guard: &impl CancelGuardTrait,
        on_cancel: C,
    ) -> Cancelable<Self, C>;
}

impl<F: Future> FutureExtension for F {
    fn cancelable<C: Fn() -> F::Error>(
        self,
        guard: &impl CancelGuardTrait,
        on_cancel: C,
    ) -> Cancelable<Self, C> {
        let (canceler, canceled) = oneshot::channel();
        guard.add_canceler(canceler);
        Cancelable {
            cancelable: self,
            canceled,
            on_cancel,
        }
    }
}
