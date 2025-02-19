use async_graphql::SimpleObject;

#[derive(Clone, Debug, SimpleObject)]
pub struct ConfigCheckResponse {
    /// Checks if the config file is validated.
    pub config_validated: bool,
    /// Checks if the subgraph settings config set by GRAPH_EXPERIMENTAL_SUBGRAPH_SETTINGS are validated.
    pub subgraph_settings_validated: bool,
    /// Returns the Config file as a string.
    pub config: String,
}

impl ConfigCheckResponse {
    pub fn from(config_validated: bool, subgraph_settings_validated: bool, config: String) -> Self {
        Self {
            config_validated,
            subgraph_settings_validated,
            config,
        }
    }
}
