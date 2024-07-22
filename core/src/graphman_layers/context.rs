use crate::graphman_primitives::{DynamicContext, ExtensibleGraphmanContext};

/// The data that is shared by all layers.
pub struct GraphmanLayerContext<Ctx> {
    inner: Ctx,
    dynamic: DynamicContext,
}

impl<Ctx> GraphmanLayerContext<Ctx> {
    /// Creates a new shared context.
    pub fn new(inner: Ctx) -> Self {
        Self {
            inner,
            dynamic: DynamicContext::new(),
        }
    }
}

impl<Ctx> AsRef<Ctx> for GraphmanLayerContext<Ctx> {
    fn as_ref(&self) -> &Ctx {
        &self.inner
    }
}

impl<Ctx> AsMut<Ctx> for GraphmanLayerContext<Ctx> {
    fn as_mut(&mut self) -> &mut Ctx {
        &mut self.inner
    }
}

impl<Ctx> ExtensibleGraphmanContext for GraphmanLayerContext<Ctx> {
    fn extend<T>(&mut self, extension: T)
    where
        T: Send + Sync + 'static,
    {
        self.dynamic.extend(extension);
    }

    fn get<T>(&self) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        self.dynamic.get()
    }
}
