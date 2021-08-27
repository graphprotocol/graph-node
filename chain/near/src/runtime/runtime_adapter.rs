use crate::{Chain, DataSource};
use anyhow::Error;
use blockchain::HostFn;
use graph::blockchain;

pub struct RuntimeAdapter {}

impl blockchain::RuntimeAdapter<Chain> for RuntimeAdapter {
    fn host_fns(&self, ds: &DataSource) -> Result<Vec<HostFn>, Error> {
        Ok(vec![])
    }
}
