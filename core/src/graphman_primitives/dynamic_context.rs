use std::any::{Any, TypeId};
use std::collections::HashMap;

use crate::graphman_primitives::ExtensibleGraphmanContext;

/// Provides a way for command extensions (layers) to share additional
/// command execution data with other layers.
pub struct DynamicContext {
    inner: HashMap<TypeId, Box<dyn Any + Send + Sync + 'static>>,
}

impl DynamicContext {
    /// Creates a new empty context.
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }
}

impl ExtensibleGraphmanContext for DynamicContext {
    fn extend<T>(&mut self, extension: T)
    where
        T: Send + Sync + 'static,
    {
        self.inner.insert(TypeId::of::<T>(), Box::new(extension));
    }

    fn get<T>(&self) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        self.inner
            .get(&TypeId::of::<T>())
            .and_then(|boxed| boxed.downcast_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    struct A(i32);

    #[derive(Debug, PartialEq, Eq)]
    struct B(i32);

    #[test]
    fn access_undefined_extensions() {
        let ctx = DynamicContext::new();

        assert_eq!(ctx.get::<A>(), None);
        assert_eq!(ctx.get::<B>(), None);
    }

    #[test]
    fn access_defined_extensions() {
        let mut ctx = DynamicContext::new();

        ctx.extend(A(1));
        ctx.extend(B(2));

        assert_eq!(ctx.get::<A>(), Some(&A(1)));
        assert_eq!(ctx.get::<B>(), Some(&B(2)));
    }

    #[test]
    fn overwrite_extensions() {
        let mut ctx = DynamicContext::new();

        ctx.extend(A(1));
        ctx.extend(A(2));

        assert_eq!(ctx.get::<A>(), Some(&A(2)));
    }
}
