use std::sync::Arc;

use graph::{
    components::store::EntityType,
    prelude::{anyhow::anyhow, DeploymentHash, Error},
};
use graph_store_postgres::SubgraphStore;

pub async fn analyze(
    store: Arc<SubgraphStore>,
    hash: String,
    entity_name: String,
) -> Result<(), Error> {
    println!("Running ANALYZE for {entity_name} entity");
    let entity_type = EntityType::new(entity_name);
    let deployment_hash =
        DeploymentHash::new(hash).expect("Subgraph hash must be a valid IPFS hash");
    store
        .analyze(&deployment_hash, entity_type)
        .await
        .map_err(|e| anyhow!(e))
}
