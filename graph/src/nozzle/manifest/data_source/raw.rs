use std::{collections::HashSet, sync::LazyLock};

use alloy::{
    json_abi::JsonAbi,
    primitives::{Address, BlockNumber},
};
use anyhow::anyhow;
use arrow::datatypes::Schema;
use futures03::future::try_join_all;
use semver::Version;
use serde::Deserialize;
use slog::Logger;
use thiserror::Error;

use super::{Abi, DataSource, Source, Table, Transformer};
use crate::{
    components::link_resolver::{LinkResolver, LinkResolverContext},
    data::subgraph::DeploymentHash,
    nozzle::{
        self,
        common::{column_aliases, Ident},
        error::IsDeterministic,
        sql::Query,
    },
};

/// Supported API versions for data source transformers.
static API_VERSIONS: LazyLock<HashSet<Version>> =
    LazyLock::new(|| HashSet::from([Version::new(0, 0, 1)]));

/// Represents an unmodified input data source of a Nozzle Subgraph.
///
/// May contain invalid or partial data.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawDataSource {
    /// The name of the data source.
    ///
    /// Must be a valid, non-empty identifier with no spaces or special characters.
    pub name: String,

    /// The kind of the data source.
    ///
    /// Must be equal to `nozzle`.
    pub kind: String,

    /// Contains sources used by this data source.
    pub source: RawSource,

    /// Contains transformations of source tables indexed by the Subgraph.
    pub transformer: RawTransformer,
}

impl RawDataSource {
    /// Parses, formats, and resolves the input data source into a valid data source.
    pub async fn resolve(
        self,
        logger: &Logger,
        link_resolver: &dyn LinkResolver,
        nozzle_client: &impl nozzle::Client,
    ) -> Result<DataSource, Error> {
        let Self {
            name,
            kind,
            source,
            transformer,
        } = self;

        let name = Self::resolve_name(name)?;
        Self::resolve_kind(kind)?;

        let source = source
            .resolve()
            .map_err(|e| e.source_context("invalid `source`"))?;

        let transformer = transformer
            .resolve(logger, link_resolver, nozzle_client, &source)
            .await
            .map_err(|e| e.source_context("invalid `transformer`"))?;

        Ok(DataSource {
            name,
            source,
            transformer,
        })
    }

    fn resolve_name(name: String) -> Result<Ident, Error> {
        Ident::new(name).map_err(|e| Error::InvalidValue(e.context("invalid `name`")))
    }

    fn resolve_kind(kind: String) -> Result<(), Error> {
        if !kind.eq_ignore_ascii_case(DataSource::KIND) {
            return Err(Error::InvalidValue(anyhow!("invalid `kind`")));
        }

        Ok(())
    }
}

/// Contains an unmodified input source used by the data source.
///
/// May contain invalid or partial data.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawSource {
    /// The dataset that SQL queries in the data source can query.
    ///
    /// Must reference a valid dataset name from the Nozzle server.
    pub dataset: String,

    /// The tables that SQL queries in the data source can query.
    ///
    /// Must reference valid table names of the dataset from the Nozzle server.
    pub tables: Vec<String>,

    /// The contract address used by SQL queries in the data source.
    ///
    /// Enables SQL query reuse through `sg_source_address()` calls instead of hard-coding the contract address.
    /// SQL queries resolve `sg_source_address()` calls to this contract address.
    pub address: Option<Address>,

    /// The minimum block number that SQL queries in the data source can query.
    pub start_block: Option<BlockNumber>,

    /// The maximum block number that SQL queries in the data source can query.
    pub end_block: Option<BlockNumber>,
}

impl RawSource {
    /// Parses, formats, and resolves the input source into a valid source.
    fn resolve(self) -> Result<Source, Error> {
        let Self {
            dataset,
            tables,
            address,
            start_block,
            end_block,
        } = self;
        let dataset = Self::resolve_dataset(dataset)?;
        let tables = Self::resolve_tables(tables)?;
        let address = address.unwrap_or(Address::ZERO);
        let start_block = start_block.unwrap_or(BlockNumber::MIN);
        let end_block = end_block.unwrap_or(BlockNumber::MAX);

        if start_block >= end_block {
            return Err(Error::InvalidValue(anyhow!(
                "`end_block` must be greater than `start_block`"
            )));
        }

        Ok(Source {
            dataset,
            tables,
            address,
            start_block,
            end_block,
        })
    }

    fn resolve_dataset(dataset: String) -> Result<Ident, Error> {
        Ident::new(dataset).map_err(|e| Error::InvalidValue(e.context("invalid `dataset`")))
    }

    fn resolve_tables(tables: Vec<String>) -> Result<Vec<Ident>, Error> {
        const MAX_TABLES: usize = 100;

        if tables.is_empty() {
            return Err(Error::InvalidValue(anyhow!("`tables` cannot be empty")));
        }

        if tables.len() > MAX_TABLES {
            return Err(Error::InvalidValue(anyhow!(
                "`tables` cannot have more than {MAX_TABLES} tables"
            )));
        }

        tables
            .into_iter()
            .enumerate()
            .map(|(i, table)| {
                Ident::new(table).map_err(|e| {
                    Error::InvalidValue(e.context(format!("invalid `tables` at index {i}")))
                })
            })
            .collect()
    }
}

/// Contains unmodified input transformations of source tables indexed by the Subgraph.
///
/// May contain invalid or partial data.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawTransformer {
    /// The version of this transformer.
    ///
    /// Must be a supported API version of the Nozzle Subgraph transformers API.
    pub api_version: Version,

    /// The ABIs that SQL queries can reference to extract event signatures.
    ///
    /// SQL queries resolve `sg_event_signature('CONTRACT_NAME', 'EVENT_NAME')` calls
    /// to full event signatures based on this list.
    pub abis: Option<Vec<RawAbi>>,

    /// The transformed tables that extract data from source tables for indexing.
    pub tables: Vec<RawTable>,
}

impl RawTransformer {
    /// Parses, formats, and resolves the input transformer into a valid transformer.
    async fn resolve(
        self,
        logger: &Logger,
        link_resolver: &dyn LinkResolver,
        nozzle_client: &impl nozzle::Client,
        source: &Source,
    ) -> Result<Transformer, Error> {
        let Self {
            api_version,
            abis,
            tables,
        } = self;
        let _api_version = Self::resolve_api_version(api_version)?;
        let abis = Self::resolve_abis(logger, link_resolver, abis).await?;
        let tables =
            Self::resolve_tables(logger, link_resolver, nozzle_client, tables, source, &abis)
                .await?;

        Ok(Transformer { abis, tables })
    }

    fn resolve_api_version(api_version: Version) -> Result<Version, Error> {
        if !API_VERSIONS.contains(&api_version) {
            return Err(Error::InvalidValue(anyhow!("invalid `api_version`")));
        }

        Ok(api_version)
    }

    async fn resolve_abis(
        logger: &Logger,
        link_resolver: &dyn LinkResolver,
        abis: Option<Vec<RawAbi>>,
    ) -> Result<Vec<Abi>, Error> {
        const MAX_ABIS: usize = 100;

        let Some(abis) = abis else {
            return Ok(Vec::new());
        };

        if abis.len() > MAX_ABIS {
            return Err(Error::InvalidValue(anyhow!(
                "`abis` cannot have more than {MAX_ABIS} ABIs"
            )));
        }

        let abi_futs = abis.into_iter().enumerate().map(|(i, abi)| async move {
            abi.resolve(logger, link_resolver)
                .await
                .map_err(|e| e.source_context(format!("invalid `abis` at index {i}")))
        });

        try_join_all(abi_futs).await
    }

    async fn resolve_tables(
        logger: &Logger,
        link_resolver: &dyn LinkResolver,
        nozzle_client: &impl nozzle::Client,
        tables: Vec<RawTable>,
        source: &Source,
        abis: &[Abi],
    ) -> Result<Vec<Table>, Error> {
        const MAX_TABLES: usize = 100;

        if tables.is_empty() {
            return Err(Error::InvalidValue(anyhow!("`tables` cannot be empty")));
        }

        if tables.len() > MAX_TABLES {
            return Err(Error::InvalidValue(anyhow!(
                "`tables` cannot have more than {MAX_TABLES} tables"
            )));
        }

        let table_futs = tables.into_iter().enumerate().map(|(i, table)| async move {
            table
                .resolve(logger, link_resolver, nozzle_client, source, abis)
                .await
                .map_err(|e| e.source_context(format!("invalid `tables` at index {i}")))
        });

        try_join_all(table_futs).await
    }
}

/// Represents an unmodified input ABI of a smart contract.
///
/// May contain invalid or partial data.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawAbi {
    /// The name of the contract.
    pub name: String,

    /// The IPFS link to the JSON ABI of the contract.
    pub file: String,
}

impl RawAbi {
    /// Parses, formats, and resolves the input ABI into a valid ABI.
    async fn resolve(
        self,
        logger: &Logger,
        link_resolver: &dyn LinkResolver,
    ) -> Result<Abi, Error> {
        let Self { name, file } = self;
        let name = Self::resolve_name(name)?;
        let contract = Self::resolve_contract(logger, link_resolver, file).await?;

        Ok(Abi { name, contract })
    }

    fn resolve_name(name: String) -> Result<Ident, Error> {
        Ident::new(name).map_err(|e| Error::InvalidValue(e.context("invalid `name`")))
    }

    async fn resolve_contract(
        logger: &Logger,
        link_resolver: &dyn LinkResolver,
        file: String,
    ) -> Result<JsonAbi, Error> {
        if file.is_empty() {
            return Err(Error::InvalidValue(anyhow!("`file` cannot be empty")));
        }

        let file_bytes = link_resolver
            .cat(
                &LinkResolverContext::new(&DeploymentHash::default(), &logger),
                &(file.into()),
            )
            .await
            .map_err(|e| Error::FailedToResolveFile(e.context("invalid `file`")))?;

        let contract: JsonAbi = serde_json::from_slice(&file_bytes)
            .map_err(|e| Error::InvalidValue(anyhow!(e).context("invalid `file`")))?;

        Ok(contract)
    }
}

/// Represents an unmodified input transformed table that extracts data from source tables for indexing.
///
/// May contain invalid or partial data.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawTable {
    /// The name of the transformed table.
    ///
    /// Must reference a valid entity name from the Subgraph schema.
    pub name: String,

    /// The SQL query that executes on the Nozzle server.
    ///
    /// Transforms the execution results into Subgraph entities.
    pub query: Option<String>,

    /// The IPFS link to the SQL query that executes on the Nozzle server.
    ///
    /// Transforms the execution results into Subgraph entities.
    ///
    /// Ignored when `query` is set.
    pub file: Option<String>,
}

impl RawTable {
    /// Parses, formats, and resolves the input table into a valid transformed table.
    async fn resolve(
        self,
        logger: &Logger,
        link_resolver: &dyn LinkResolver,
        nozzle_client: &impl nozzle::Client,
        source: &Source,
        abis: &[Abi],
    ) -> Result<Table, Error> {
        let Self { name, query, file } = self;
        let name = Self::resolve_name(name)?;
        let query = match Self::resolve_query(query, source, abis)? {
            Some(query) => query,
            None => Self::resolve_file(logger, link_resolver, file, source, abis).await?,
        };
        let schema = Self::resolve_schema(logger, nozzle_client, &query).await?;

        Ok(Table {
            name,
            query,
            schema,
        })
    }

    fn resolve_name(name: String) -> Result<Ident, Error> {
        Ident::new(name).map_err(|e| Error::InvalidValue(e.context("invalid `name`")))
    }

    fn resolve_query(
        query: Option<String>,
        source: &Source,
        abis: &[Abi],
    ) -> Result<Option<Query>, Error> {
        let Some(query) = query else {
            return Ok(None);
        };

        if query.is_empty() {
            return Err(Error::InvalidValue(anyhow!("`query` cannot be empty")));
        }

        Query::new(
            query,
            &source.dataset,
            &source.tables,
            &source.address,
            abis.iter().map(|abi| (&abi.name, &abi.contract)),
        )
        .map(Some)
        .map_err(|e| Error::InvalidValue(e.context("invalid `query`")))
    }

    async fn resolve_file(
        logger: &Logger,
        link_resolver: &dyn LinkResolver,
        file: Option<String>,
        source: &Source,
        abis: &[Abi],
    ) -> Result<Query, Error> {
        let Some(file) = file else {
            return Err(Error::InvalidValue(anyhow!("`file` cannot be empty")));
        };

        if file.is_empty() {
            return Err(Error::InvalidValue(anyhow!("`file` cannot be empty")));
        }

        let file_bytes = link_resolver
            .cat(
                &LinkResolverContext::new(&DeploymentHash::default(), logger),
                &(file.into()),
            )
            .await
            .map_err(|e| Error::FailedToResolveFile(e.context("invalid `file`")))?;

        let query = String::from_utf8(file_bytes)
            .map_err(|e| Error::InvalidValue(anyhow!(e).context("invalid `file`")))?;

        if query.is_empty() {
            return Err(Error::InvalidValue(anyhow!("`file` cannot be empty")));
        }

        Query::new(
            query,
            &source.dataset,
            &source.tables,
            &source.address,
            abis.iter().map(|abi| (&abi.name, &abi.contract)),
        )
        .map_err(|e| Error::InvalidValue(e.context("invalid `file`")))
    }

    async fn resolve_schema(
        logger: &Logger,
        nozzle_client: &impl nozzle::Client,
        query: &Query,
    ) -> Result<Schema, Error> {
        let schema = nozzle_client.schema(logger, &query).await.map_err(|e| {
            Error::FailedToExecuteQuery {
                is_deterministic: e.is_deterministic(),
                source: anyhow!(e).context("failed to load schema"),
            }
        })?;

        let check_required_column = |c: &[&str], kind: &str| {
            if !c.iter().any(|&c| schema.column_with_name(c).is_some()) {
                return Err(Error::InvalidQuery(anyhow!("query must return {kind}")));
            }
            Ok(())
        };

        check_required_column(column_aliases::BLOCK_NUMBER, "block numbers")?;
        check_required_column(column_aliases::BLOCK_HASH, "block hashes")?;
        check_required_column(column_aliases::BLOCK_TIMESTAMP, "block timestamps")?;

        Ok(schema)
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid value: {0:#}")]
    InvalidValue(#[source] anyhow::Error),

    #[error("invalid query: {0:#}")]
    InvalidQuery(#[source] anyhow::Error),

    #[error("failed to resolve file: {0:#}")]
    FailedToResolveFile(#[source] anyhow::Error),

    #[error("failed to execute query: {source:#}")]
    FailedToExecuteQuery {
        source: anyhow::Error,
        is_deterministic: bool,
    },
}

impl Error {
    /// Extends the source errors with additional context keeping the original error kind and the determinism.
    fn source_context(self, cx: impl Into<String>) -> Self {
        match self {
            Self::InvalidValue(e) => Self::InvalidValue(e.context(cx.into())),
            Self::InvalidQuery(e) => Self::InvalidQuery(e.context(cx.into())),
            Self::FailedToResolveFile(e) => Self::FailedToResolveFile(e.context(cx.into())),
            Self::FailedToExecuteQuery {
                source,
                is_deterministic,
            } => Self::FailedToExecuteQuery {
                source: source.context(cx.into()),
                is_deterministic,
            },
        }
    }
}

impl IsDeterministic for Error {
    fn is_deterministic(&self) -> bool {
        match self {
            Self::InvalidValue(_) => true,
            Self::InvalidQuery(_) => true,
            Self::FailedToResolveFile(_) => false,
            Self::FailedToExecuteQuery {
                is_deterministic, ..
            } => *is_deterministic,
        }
    }
}
