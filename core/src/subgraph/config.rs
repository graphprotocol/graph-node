use graph::prelude::{regex::Regex, SubgraphName};

#[derive(Default, Debug)]
pub struct SubgraphPerfRules {
    pub rules: Vec<(Regex, SubgraphPerfConfig)>,
}

impl SubgraphPerfRules {
    pub fn config_for_name(&self, name: &SubgraphName) -> Option<SubgraphPerfConfig> {
        self.rules.iter().find_map(|(regex, config)| {
            if regex.is_match(name.as_str()) {
                Some(config.clone())
            } else {
                None
            }
        })
    }
}

#[derive(Default, Debug, Clone)]
// TODO: Remove clone and use Arc or something on the RulesStructure
// Make this struct combinable by setting relevant priority per field,
// eg prefer shorter history_blocks etc
pub struct SubgraphPerfConfig {
    pub history_blocks: i32,
}
