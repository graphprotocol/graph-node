use crate::capabilities::NodeCapabilities;
use crate::{data_source::DataSource, Chain};
use graph::blockchain as bc;
use graph::prelude::*;
use std::collections::HashSet;

const MATCH_ALL_WILDCARD: &str = "";
#[derive(Clone, Debug, Default)]
pub struct TriggerFilter {
    pub(crate) block_filter: ArweaveBlockFilter,
    pub(crate) transaction_filter: ArweaveTransactionFilter,
}

impl bc::TriggerFilter<Chain> for TriggerFilter {
    fn extend<'a>(&mut self, data_sources: impl Iterator<Item = &'a DataSource> + Clone) {
        let TriggerFilter {
            block_filter,
            transaction_filter,
        } = self;

        block_filter.extend(ArweaveBlockFilter::from_data_sources(data_sources.clone()));
        transaction_filter.extend(ArweaveTransactionFilter::from_data_sources(data_sources));
    }

    fn node_capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {}
    }

    fn extend_with_template(
        &mut self,
        _data_source: impl Iterator<Item = <Chain as bc::Blockchain>::DataSourceTemplate>,
    ) {
    }

    fn to_firehose_filter(self) -> Vec<prost_types::Any> {
        vec![]
    }
}

/// ArweaveBlockFilter will match every block regardless of source being set.
/// see docs: https://thegraph.com/docs/en/supported-networks/arweave/
#[derive(Clone, Debug, Default)]
pub(crate) struct ArweaveTransactionFilter {
    owners: HashSet<Vec<u8>>,
    match_all: bool,
}

impl ArweaveTransactionFilter {
    pub fn matches(&self, owner: &[u8]) -> bool {
        self.match_all || self.owners.contains(owner)
    }

    pub fn from_data_sources<'a>(iter: impl IntoIterator<Item = &'a DataSource>) -> Self {
        let owners: Vec<Vec<u8>> = iter
            .into_iter()
            .filter(|data_source| {
                data_source.source.owner.is_some()
                    && !data_source.mapping.transaction_handlers.is_empty()
            })
            .map(|ds| match &ds.source.owner {
                Some(str) if MATCH_ALL_WILDCARD.eq(str) => str.clone().into_bytes(),
                owner @ _ => {
                    base64_url::decode(&owner.clone().unwrap_or_default()).unwrap_or_default()
                }
            })
            .collect();

        let owners = HashSet::from_iter(owners);
        Self {
            match_all: owners.contains(MATCH_ALL_WILDCARD.as_bytes()),
            owners,
        }
    }

    pub fn extend(&mut self, other: ArweaveTransactionFilter) {
        let ArweaveTransactionFilter { owners, match_all } = self;

        owners.extend(other.owners);
        *match_all = *match_all || other.match_all;
    }
}

/// ArweaveBlockFilter will match every block regardless of source being set.
/// see docs: https://thegraph.com/docs/en/supported-networks/arweave/
#[derive(Clone, Debug, Default)]
pub(crate) struct ArweaveBlockFilter {
    pub trigger_every_block: bool,
}

impl ArweaveBlockFilter {
    pub fn from_data_sources<'a>(iter: impl IntoIterator<Item = &'a DataSource>) -> Self {
        Self {
            trigger_every_block: iter
                .into_iter()
                .any(|data_source| !data_source.mapping.block_handlers.is_empty()),
        }
    }

    pub fn extend(&mut self, other: ArweaveBlockFilter) {
        self.trigger_every_block = self.trigger_every_block || other.trigger_every_block;
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashSet, sync::Arc};

    use graph::{prelude::Link, semver::Version};

    use crate::data_source::{DataSource, Mapping, Source, TransactionHandler};

    use super::{ArweaveTransactionFilter, MATCH_ALL_WILDCARD};

    #[test]
    fn transaction_filter_wildcard_matches_all() {
        let dss = vec![
            new_datasource(None, 10),
            new_datasource(Some(MATCH_ALL_WILDCARD.into()), 10),
            new_datasource(Some("owner".into()), 10),
        ];

        let dss: Vec<&DataSource> = dss.iter().collect();

        let filter = ArweaveTransactionFilter::from_data_sources(dss);
        assert_eq!(true, filter.matches("asdas".as_bytes()))
    }

    #[test]
    fn transaction_filter_match() {
        let dss = vec![
            new_datasource(None, 10),
            new_datasource(Some("owner".into()), 10),
        ];

        let x: Vec<&DataSource> = dss.iter().collect();

        let filter = ArweaveTransactionFilter::from_data_sources(x);
        assert_eq!(
            false,
            filter.matches(base64_url::encode(&"asdas".to_string()).as_bytes())
        );
        assert_eq!(true, filter.matches("owner".as_bytes()))
    }

    #[test]
    fn transaction_filter_extend_wildcard_matches_all() {
        let dss = vec![
            new_datasource(None, 10),
            new_datasource(Some(MATCH_ALL_WILDCARD.into()), 10),
            new_datasource(Some("owner".into()), 10),
        ];

        let dss: Vec<&DataSource> = dss.iter().collect();

        let mut filter = ArweaveTransactionFilter {
            owners: HashSet::default(),
            match_all: false,
        };

        filter.extend(ArweaveTransactionFilter::from_data_sources(dss));
        assert_eq!(true, filter.matches("asdas".as_bytes()))
    }

    fn new_datasource(owner: Option<String>, start_block: i32) -> DataSource {
        DataSource {
            kind: "".into(),
            network: None,
            name: "".into(),
            source: Source {
                owner: owner.map(|owner| base64_url::encode(&owner)),
                start_block,
            },
            mapping: Mapping {
                api_version: Version::new(1, 2, 3),
                language: "".into(),
                entities: vec![],
                block_handlers: vec![],
                transaction_handlers: vec![TransactionHandler {
                    handler: "my_handler".into(),
                }],
                runtime: Arc::new(vec![]),
                link: Link { link: "".into() },
            },
            context: Arc::new(None),
            creation_block: None,
        }
    }
}
