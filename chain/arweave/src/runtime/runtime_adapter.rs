use blockchain::HostFn;
use graph::anyhow::Error;
use graph::blockchain;

use crate::data_source::DataSource;
use crate::Chain;

pub struct RuntimeAdapter {}

impl blockchain::RuntimeAdapter<Chain> for RuntimeAdapter {
    fn host_fns(&self, _ds: &DataSource) -> Result<Vec<HostFn>, Error> {
        Ok(vec![])
    }
}
