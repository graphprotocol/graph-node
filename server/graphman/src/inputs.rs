#[derive(juniper::GraphQLInputObject)]
pub struct ConfigInput {
    pub postgres_url: String,
    pub node_id: Option<String>,
    pub ipfs: Option<Vec<String>>,
    pub fork_base: Option<String>,
    pub version_label: Option<String>,
    pub current: Option<bool>,
    pub pending: Option<bool>,
    pub status: Option<bool>,
    pub used: Option<bool>,
    pub all: Option<bool>,
}
