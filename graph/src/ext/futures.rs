use futures::sync::oneshot;
use std::sync::{Arc, Mutex};
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
    fn add_canceler(&mut self, canceler: oneshot::Sender<()>);
}

/// Cancels any guarded futures and streams when dropped.
#[derive(Debug, Default)]
pub struct CancelGuard {
    cancelers: Vec<oneshot::Sender<()>>,
}

impl CancelGuard {
    /// Creates a guard that initially guards nothing.
    pub fn new() -> Self {
        Self::default()
    }

    /// A more readable `drop`.
    pub fn cancel(self) {}

    /// Convert into a `SharedGuard`.
    pub fn shared(self) -> SharedCancelGuard {
        SharedCancelGuard {
            guard: Arc::new(Mutex::new(Some(self))),
        }
    }
}

impl CancelGuardTrait for CancelGuard {
    fn add_canceler(&mut self, canceler: oneshot::Sender<()>) {
        self.cancelers.push(canceler);
    }
}

/// A version of `CancelGuard` that can be cloned.
///
/// To cancel the stream, call `cancel` on one of the guards or drop all guards.
#[derive(Clone, Debug)]
pub struct SharedCancelGuard {
    guard: Arc<Mutex<Option<CancelGuard>>>,
}

impl SharedCancelGuard {
    /// Cancels the stream, a noop if already canceled.
    pub fn cancel(&self) {
        *self.guard.lock().unwrap() = None
    }

    pub fn is_canceled(&self) -> bool {
        self.guard.lock().unwrap().is_none()
    }
}

impl CancelGuardTrait for SharedCancelGuard {
    /// A noop if `self` has already been canceled.
    fn add_canceler(&mut self, canceler: oneshot::Sender<()>) {
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
        guard: &mut impl CancelGuardTrait,
        on_cancel: C,
    ) -> Cancelable<Self, C>;
}

impl<S: Stream> StreamExtension for S {
    fn cancelable<C: Fn() -> S::Error>(
        self,
        guard: &mut impl CancelGuardTrait,
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
        guard: &mut impl CancelGuardTrait,
        on_cancel: C,
    ) -> Cancelable<Self, C>;
}

impl<F: Future> FutureExtension for F {
    fn cancelable<C: Fn() -> F::Error>(
        self,
        guard: &mut impl CancelGuardTrait,
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
