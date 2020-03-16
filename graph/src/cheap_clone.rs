use slog::Logger;
use std::rc::Rc;
use std::sync::Arc;

/// Things that, in the context of an application such as Graph Node, are fast to clone.
pub trait CheapClone: Clone {
    fn cheap_clone(&self) -> Self {
        self.clone()
    }
}

impl<T: ?Sized> CheapClone for Rc<T> {}
impl<T: ?Sized> CheapClone for Arc<T> {}
impl CheapClone for Logger {}
