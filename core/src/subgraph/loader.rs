use std::time::Instant;

use graph::blockchain::{Blockchain, DataSource, DataSourceTemplate as _};
use graph::components::store::WritableStore;
use graph::prelude::*;

pub async fn load_dynamic_data_sources<C: Blockchain>(
    store: Arc<dyn WritableStore>,
    logger: Logger,
    manifest: &SubgraphManifest<C>,
    manifest_idx_and_name: Vec<(u32, String)>,
) -> Result<Vec<C::DataSource>, Error> {
    let start_time = Instant::now();

    let mut data_sources: Vec<C::DataSource> = vec![];

    for stored in store
        .load_dynamic_data_sources(manifest_idx_and_name)
        .await?
    {
        let template = manifest
            .templates
            .iter()
            .find(|template| template.manifest_idx() == stored.manifest_idx)
            .ok_or_else(|| anyhow!("no template with idx `{}` was found", stored.manifest_idx))?;

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
