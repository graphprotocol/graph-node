use crate::{Chain, DataSource};
use anyhow::Result;
use blockchain::HostFn;
use graph::blockchain;

pub struct RuntimeAdapter {}

impl blockchain::RuntimeAdapter<Chain> for RuntimeAdapter {
    fn host_fns(&self, _ds: &DataSource) -> Result<Vec<HostFn>> {
        Ok(vec![])
    }
}
