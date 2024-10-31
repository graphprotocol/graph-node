use std::marker::PhantomData;

use crate::data_source;

use super::{Blockchain, HostFn, RuntimeAdapter};

/// A [`RuntimeAdapter`] that does not expose any host functions.
#[derive(Debug, Clone)]
pub struct NoopRuntimeAdapter<C>(PhantomData<C>);

impl<C> Default for NoopRuntimeAdapter<C> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<C> RuntimeAdapter<C> for NoopRuntimeAdapter<C>
where
    C: Blockchain,
{
    fn host_fns(&self, _ds: &data_source::DataSource<C>) -> anyhow::Result<Vec<HostFn>> {
        Ok(vec![])
    }
}
