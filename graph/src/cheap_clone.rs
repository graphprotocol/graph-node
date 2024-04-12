use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use tonic::transport::Channel;

/// Things that are fast to clone in the context of an application such as
/// Graph Node
///
/// The purpose of this API is to reduce the number of calls to .clone()
/// which need to be audited for performance.
///
/// In general, the derive macro `graph::Derive::CheapClone` should be used
/// to implement this trait. A manual implementation should only be used if
/// the derive macro cannot be used, and should mention all fields that need
/// to be cloned.
///
/// As a rule of thumb, only constant-time Clone impls should also implement
/// CheapClone.
/// Eg:
///    ✔ Arc<T>
///    ✗ Vec<T>
///    ✔ u128
///    ✗ String
pub trait CheapClone: Clone {
    fn cheap_clone(&self) -> Self;
}

impl<T: ?Sized> CheapClone for Rc<T> {
    #[inline]
    fn cheap_clone(&self) -> Self {
        self.clone()
    }
}

impl<T: ?Sized> CheapClone for Arc<T> {
    #[inline]
    fn cheap_clone(&self) -> Self {
        self.clone()
    }
}

impl<T: ?Sized + CheapClone> CheapClone for Box<T> {
    #[inline]
    fn cheap_clone(&self) -> Self {
        self.clone()
    }
}

impl<T: ?Sized + CheapClone> CheapClone for std::pin::Pin<T> {
    #[inline]
    fn cheap_clone(&self) -> Self {
        self.clone()
    }
}

impl<T: CheapClone> CheapClone for Option<T> {
    #[inline]
    fn cheap_clone(&self) -> Self {
        self.clone()
    }
}

// Pool is implemented as a newtype over Arc,
// So it is CheapClone.
impl<M: diesel::r2d2::ManageConnection> CheapClone for diesel::r2d2::Pool<M> {
    #[inline]
    fn cheap_clone(&self) -> Self {
        self.clone()
    }
}

impl<F: Future> CheapClone for futures03::future::Shared<F> {
    #[inline]
    fn cheap_clone(&self) -> Self {
        self.clone()
    }
}

macro_rules! cheap_clone_is_clone {
    ($($t:ty),*) => {
        $(
            impl CheapClone for $t {
                #[inline]
                fn cheap_clone(&self) -> Self {
                    self.clone()
                }
            }
        )*
    };
}

macro_rules! cheap_clone_is_copy {
    ($($t:ty),*) => {
        $(
            impl CheapClone for $t {
                #[inline]
                fn cheap_clone(&self) -> Self {
                    *self
                }
            }
        )*
    };
}

cheap_clone_is_clone!(Channel);
// reqwest::Client uses Arc internally, so it is CheapClone.
cheap_clone_is_clone!(reqwest::Client);
cheap_clone_is_clone!(slog::Logger);

cheap_clone_is_copy!(
    (),
    bool,
    u16,
    u32,
    i32,
    u64,
    usize,
    &'static str,
    std::time::Duration
);
cheap_clone_is_copy!(ethabi::Address);
