use async_graphql::SimpleObject;

#[derive(Clone, Debug, SimpleObject)]
pub struct PlaceResponse {
    subgraph: String,
    network: String,
    shards: Vec<String>,
    nodes: Vec<String>,
}

impl PlaceResponse {
    pub fn from(
        subgraph: String,
        network: String,
        shards: Vec<String>,
        nodes: Vec<String>,
    ) -> Self {
        Self {
            subgraph,
            network,
            shards,
            nodes,
        }
    }
}
