use crate::{data_source::DataSource, Chain};
use blockchain::HostFn;
use graph::{anyhow::Error, blockchain};

pub struct RuntimeAdapter {}

impl blockchain::RuntimeAdapter<Chain> for RuntimeAdapter {
    fn host_fns(&self, _ds: &DataSource) -> Result<Vec<HostFn>, Error> {
        Ok(vec![])
    }
}
