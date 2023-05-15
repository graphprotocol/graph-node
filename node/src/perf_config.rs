use std::collections::HashMap;

use graph::{anyhow, prelude::regex::Regex};
use graph_core::{SubgraphPerfConfig, SubgraphPerfRules};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SubgraphConfigSection {
    rules: HashMap<String, SubgraphConfigRule>,
}

impl TryInto<SubgraphPerfRules> for SubgraphConfigSection {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<SubgraphPerfRules, Self::Error> {
        self.rules
            .into_values()
            .map(|x| {
                let key = match x.match_rule {
                    MatchSelection::SubgraphName { name } => Regex::new(&name),
                };

                key.map(|k| (k, x.actions.into()))
                    .map_err(anyhow::Error::from)
            })
            .collect::<anyhow::Result<Vec<(Regex, SubgraphPerfConfig)>>>()
            .map(|rules| SubgraphPerfRules { rules })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SubgraphConfigRule {
    #[serde(alias = "match")]
    match_rule: MatchSelection,
    actions: SubgraphConfigDetails,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct SubgraphConfigDetails {
    history_blocks: i32,
}

impl Into<SubgraphPerfConfig> for SubgraphConfigDetails {
    fn into(self) -> SubgraphPerfConfig {
        let Self { history_blocks } = self;

        SubgraphPerfConfig { history_blocks }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MatchSelection {
    SubgraphName { name: String },
}

#[cfg(test)]
mod test {
    use crate::perf_config::MatchSelection;

    #[test]
    fn parses_correctly() {
        let content = r#"
        [rules]

        [rules.name] 
        match = { type = "subgraph_name", name = ".*" } 
        actions = { history_blocks = 10000 } 

        [rules.name2]
        match = { type = "subgraph_name", name = "xxxxx" } 
        actions = { history_blocks = 10000 } 

        [rules.name3] 
        match = { type = "subgraph_name", name = ".*!$" } 
        actions = { history_blocks = 10000 } 
        "#;

        let section = toml::from_str::<super::SubgraphConfigSection>(content).unwrap();
        assert_eq!(section.rules.len(), 3);

        let rule1 = match section.rules.get("name").unwrap().match_rule {
            MatchSelection::SubgraphName { ref name } => name,
        };
        assert_eq!(rule1, ".*");

        let rule2 = match section.rules.get("name2").unwrap().match_rule {
            MatchSelection::SubgraphName { ref name } => name,
        };
        assert_eq!(rule2, "xxxxx");
        let rule1 = match section.rules.get("name3").unwrap().match_rule {
            MatchSelection::SubgraphName { ref name } => name,
        };
        assert_eq!(rule1, ".*!$");
    }
}
