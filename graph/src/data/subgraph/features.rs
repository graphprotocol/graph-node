use crate::prelude::Deserialize;

#[derive(Debug, Deserialize, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "camelCase")]
pub enum SubgraphFeature {
    NonFatalErrors,
}
