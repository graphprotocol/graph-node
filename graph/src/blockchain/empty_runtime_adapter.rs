use std::marker::PhantomData;

use super::{Blockchain, HostFn, RuntimeAdapter};

/// A [`RuntimeAdapter`] that does not expose any host functions.
#[derive(Debug, Clone)]
pub struct EmptyRuntimeAdapter<C>(PhantomData<C>);

impl<C> Default for EmptyRuntimeAdapter<C> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<C> RuntimeAdapter<C> for EmptyRuntimeAdapter<C>
where
    C: Blockchain,
{
    fn host_fns(&self, _ds: &C::DataSource) -> anyhow::Result<Vec<HostFn>> {
        Ok(vec![])
    }
}
