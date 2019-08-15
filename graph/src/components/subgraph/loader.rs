use crate::prelude::*;

pub trait DataSourceLoader {
    fn load_dynamic_data_sources(
        self: Arc<Self>,
        id: &SubgraphDeploymentId,
        logger: Logger,
    ) -> Box<dyn Future<Item = Vec<DataSource>, Error = Error> + Send>;
}
