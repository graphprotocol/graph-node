use std::time::Instant;

use graph::blockchain::{Blockchain, DataSource, DataSourceTemplate as _};
use graph::components::store::WritableStore;
use graph::prelude::*;

pub async fn load_dynamic_data_sources<C: Blockchain>(
    store: Arc<dyn WritableStore>,
    logger: Logger,
    manifest: &SubgraphManifest<C>,
) -> Result<Vec<C::DataSource>, Error> {
    let start_time = Instant::now();

    let mut data_sources: Vec<C::DataSource> = vec![];

    for stored in store.load_dynamic_data_sources().await? {
        let template = manifest
            .templates
            .iter()
            .find(|template| template.name() == stored.name.as_str())
            .ok_or_else(|| anyhow!("no template named `{}` was found", stored.name))?;

        let ds = C::DataSource::from_stored_dynamic_data_source(&template, stored)?;

        // The data sources are ordered by the creation block.
        // See also 8f1bca33-d3b7-4035-affc-fd6161a12448.
        anyhow::ensure!(
            data_sources.last().and_then(|d| d.creation_block()) <= ds.creation_block(),
            "Assertion failure: new data source has lower creation block than existing ones"
        );

        data_sources.push(ds);
    }

    trace!(
        logger,
        "Loaded dynamic data sources";
        "ms" => start_time.elapsed().as_millis()
    );

    Ok(data_sources)
}
