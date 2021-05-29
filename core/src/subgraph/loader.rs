use std::collections::HashMap;
use std::iter::FromIterator;
use std::time::Instant;

use graph::components::store::{StoredDynamicDataSource, WritableStore};
use graph::prelude::*;

pub async fn load_dynamic_data_sources(
    store: Arc<dyn WritableStore>,
    deployment_id: &DeploymentHash,
    logger: Logger,
    templates: Vec<DataSourceTemplate>,
) -> Result<Vec<graph_chain_ethereum::DataSource>, Error> {
    let start_time = Instant::now();

    let template_map: HashMap<&str, &DataSourceTemplate> = HashMap::from_iter(
        templates
            .iter()
            .map(|template| (template.name.as_str(), template)),
    );
    let mut data_sources: Vec<graph_chain_ethereum::DataSource> = vec![];

    for stored in store.load_dynamic_data_sources().await? {
        let StoredDynamicDataSource {
            name,
            source,
            context,
            creation_block,
        } = stored;

        let template = template_map.get(name.as_str()).ok_or_else(|| {
            anyhow!(
                "deployment `{}` does not have a template called `{}`",
                deployment_id.as_str(),
                name
            )
        })?;
        let context = context
            .map(|ctx| serde_json::from_str::<Entity>(&ctx))
            .transpose()?;

        let contract_abi = template.mapping.find_abi(&template.source.abi)?;

        let ds = graph_chain_ethereum::DataSource {
            kind: template.kind.clone(),
            network: template.network.clone(),
            name,
            source,
            mapping: template.mapping.clone(),
            context: Arc::new(context),
            creation_block,
            contract_abi,
        };

        // The data sources are ordered by the creation block.
        // See also 8f1bca33-d3b7-4035-affc-fd6161a12448.
        anyhow::ensure!(
            data_sources.last().and_then(|d| d.creation_block) <= ds.creation_block,
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
