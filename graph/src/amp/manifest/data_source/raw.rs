use std::{collections::HashSet, sync::LazyLock};

use alloy::{
    json_abi::JsonAbi,
    primitives::{Address, BlockNumber},
};
use anyhow::anyhow;
use arrow::{array::RecordBatch, datatypes::Schema};
use futures03::future::try_join_all;
use itertools::Itertools;
use lazy_regex::regex_is_match;
use semver::Version;
use serde::Deserialize;
use slog::{debug, error, Logger};
use thiserror::Error;

use super::{Abi, DataSource, Source, Table, Transformer};
use crate::{
    amp::{
        self,
        codec::utils::{
            auto_block_hash_decoder, auto_block_number_decoder, auto_block_timestamp_decoder,
        },
        error::IsDeterministic,
        sql::{BlockRangeQueryBuilder, ContextQuery, ValidQuery},
    },
    components::link_resolver::{LinkResolver, LinkResolverContext},
    data::subgraph::DeploymentHash,
    schema::InputSchema,
};

/// Supported API versions for data source transformers.
static API_VERSIONS: LazyLock<HashSet<Version>> =
    LazyLock::new(|| HashSet::from([Version::new(0, 0, 1)]));

/// Represents an unmodified input data source of an Amp subgraph.
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
    /// Must be equal to `amp`.
    pub kind: String,

    /// The network name of the data source.
    pub network: String,

    /// Contains sources used by this data source.
    pub source: RawSource,

    /// Contains transformations of source tables indexed by the subgraph.
    pub transformer: RawTransformer,
}

impl RawDataSource {
    /// Parses, formats, and resolves the input data source into a valid data source.
    pub async fn resolve(
        self,
        logger: &Logger,
        link_resolver: &dyn LinkResolver,
        amp_client: &impl amp::Client,
        input_schema: Option<&InputSchema>,
    ) -> Result<DataSource, Error> {
        let Self {
            name,
            kind,
            network,
            source,
            transformer,
        } = self;

        let logger = logger.new(slog::o!("data_source" => name.clone()));
        debug!(logger, "Resolving data source");

        validate_ident(&name).map_err(|e| e.source_context("invalid `name`"))?;
        Self::validate_kind(kind)?;

        let source = source
            .resolve()
            .map_err(|e| e.source_context("invalid `source`"))?;

        let transformer = transformer
            .resolve(&logger, link_resolver, amp_client, input_schema, &source)
            .await
            .map_err(|e| e.source_context("invalid `transformer`"))?;

        Ok(DataSource {
            name,
            network,
            source,
            transformer,
        })
    }

    fn validate_kind(kind: String) -> Result<(), Error> {
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
    /// Must reference a valid dataset name from the Amp server.
    pub dataset: String,

    /// The tables that SQL queries in the data source can query.
    ///
    /// Must reference valid table names of the dataset from the Amp server.
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

        if dataset.is_empty() {
            return Err(Error::InvalidValue(anyhow!("`dataset` cannot be empty")));
        }
        Self::validate_tables(&tables)?;

        let dataset = normalize_sql_ident(&dataset);
        let tables = tables
            .into_iter()
            .map(|table| normalize_sql_ident(&table))
            .collect_vec();

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

    fn validate_tables(tables: &[String]) -> Result<(), Error> {
        const MAX_TABLES: usize = 100;

        if tables.is_empty() {
            return Err(Error::InvalidValue(anyhow!("`tables` cannot be empty")));
        }

        if tables.len() > MAX_TABLES {
            return Err(Error::InvalidValue(anyhow!(
                "`tables` cannot have more than {MAX_TABLES} tables"
            )));
        }

        for (i, table) in tables.iter().enumerate() {
            if table.is_empty() {
                return Err(Error::InvalidValue(anyhow!(
                    "`table` at index {i} cannot be empty"
                )));
            }
        }

        Ok(())
    }
}

/// Contains unmodified input transformations of source tables indexed by the subgraph.
///
/// May contain invalid or partial data.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawTransformer {
    /// The version of this transformer.
    ///
    /// Must be a supported API version of the Amp subgraph transformers API.
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
        amp_client: &impl amp::Client,
        input_schema: Option<&InputSchema>,
        source: &Source,
    ) -> Result<Transformer, Error> {
        let Self {
            api_version,
            abis,
            tables,
        } = self;
        Self::validate_api_version(&api_version)?;

        let abis = Self::resolve_abis(logger, link_resolver, abis).await?;
        let tables = Self::resolve_tables(
            logger,
            link_resolver,
            amp_client,
            input_schema,
            tables,
            source,
            &abis,
        )
        .await?;

        Ok(Transformer {
            api_version,
            abis,
            tables,
        })
    }

    fn validate_api_version(api_version: &Version) -> Result<(), Error> {
        if !API_VERSIONS.contains(api_version) {
            return Err(Error::InvalidValue(anyhow!("invalid `api_version`")));
        }

        Ok(())
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
            let logger = logger.new(slog::o!("abi_name" => abi.name.clone()));
            debug!(logger, "Resolving ABI";
                "file" => &abi.file,
            );

            abi.resolve(&logger, link_resolver)
                .await
                .map_err(|e| e.source_context(format!("invalid `abis` at index {i}")))
        });

        try_join_all(abi_futs).await
    }

    async fn resolve_tables(
        logger: &Logger,
        link_resolver: &dyn LinkResolver,
        amp_client: &impl amp::Client,
        input_schema: Option<&InputSchema>,
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
            let logger = logger.new(slog::o!("table_name" => table.name.clone()));
            debug!(logger, "Resolving table";
                "file" => ?&table.file
            );

            table
                .resolve(
                    &logger,
                    link_resolver,
                    amp_client,
                    input_schema,
                    source,
                    abis,
                )
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

        validate_ident(&name).map_err(|e| e.source_context("invalid `name`"))?;
        let contract = Self::resolve_contract(logger, link_resolver, file).await?;

        Ok(Abi { name, contract })
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
                &LinkResolverContext::new(&DeploymentHash::default(), logger),
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
    /// Must reference a valid entity name from the subgraph schema.
    pub name: String,

    /// The SQL query that executes on the Amp server.
    ///
    /// Transforms the execution results into subgraph entities.
    pub query: Option<String>,

    /// The IPFS link to the SQL query that executes on the Amp server.
    ///
    /// Transforms the execution results into subgraph entities.
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
        amp_client: &impl amp::Client,
        input_schema: Option<&InputSchema>,
        source: &Source,
        abis: &[Abi],
    ) -> Result<Table, Error> {
        let Self { name, query, file } = self;

        validate_ident(&name).map_err(|e| e.source_context("invalid `name`"))?;
        let query = match Self::resolve_query(query, source, abis)? {
            Some(query) => query,
            None => Self::resolve_file(logger, link_resolver, file, source, abis).await?,
        };

        debug!(logger, "Resolving query schema");
        let schema = Self::resolve_schema(logger, amp_client, &query).await?;

        for field in schema.fields() {
            validate_ident(field.name()).map_err(|e| {
                e.source_context(format!(
                    "invalid query output schema: invalid column '{}'",
                    field.name()
                ))
            })?;
        }

        let block_range_query_builder = Self::resolve_block_range_query_builder(
            logger,
            amp_client,
            input_schema,
            source,
            query,
            schema.clone(),
        )
        .await?;

        Ok(Table {
            name,
            query: block_range_query_builder,
            schema,
        })
    }

    fn resolve_query(
        query: Option<String>,
        source: &Source,
        abis: &[Abi],
    ) -> Result<Option<ValidQuery>, Error> {
        let Some(query) = query else {
            return Ok(None);
        };

        if query.is_empty() {
            return Err(Error::InvalidValue(anyhow!("`query` cannot be empty")));
        }

        ValidQuery::new(
            &query,
            source.dataset.as_str(),
            source.tables.iter().map(|table| table.as_str()),
            &source.address,
            abis.iter().map(|abi| (abi.name.as_str(), &abi.contract)),
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
    ) -> Result<ValidQuery, Error> {
        debug!(logger, "Resolving query file");

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

        ValidQuery::new(
            &query,
            source.dataset.as_str(),
            source.tables.iter().map(|table| table.as_str()),
            &source.address,
            abis.iter().map(|abi| (abi.name.as_str(), &abi.contract)),
        )
        .map_err(|e| Error::InvalidValue(e.context("invalid `file`")))
    }

    async fn resolve_schema(
        logger: &Logger,
        amp_client: &impl amp::Client,
        query: impl ToString,
    ) -> Result<Schema, Error> {
        amp_client
            .schema(logger, query)
            .await
            .map_err(|e| Error::FailedToExecuteQuery {
                is_deterministic: e.is_deterministic(),
                source: anyhow!(e).context("failed to load schema"),
            })
    }

    async fn resolve_block_range_query_builder(
        logger: &Logger,
        amp_client: &impl amp::Client,
        input_schema: Option<&InputSchema>,
        source: &Source,
        query: ValidQuery,
        schema: Schema,
    ) -> Result<BlockRangeQueryBuilder, Error> {
        debug!(logger, "Resolving block range query builder");

        let record_batch = RecordBatch::new_empty(schema.into());
        let (block_number_column, _) =
            auto_block_number_decoder(&record_batch).map_err(Error::InvalidQuery)?;

        let need_block_hash_column = auto_block_hash_decoder(&record_batch).is_err();
        let need_block_timestamp_column = input_schema
            .map(|input_schema| input_schema.has_aggregations())
            .unwrap_or(false)
            && auto_block_timestamp_decoder(&record_batch).is_err();

        if !need_block_hash_column && !need_block_timestamp_column {
            return Ok(BlockRangeQueryBuilder::new(query, block_number_column));
        }

        debug!(logger, "Resolving context query");
        let mut context_query: Option<ContextQuery> = None;

        // TODO: Context is embedded in the original query using INNER JOIN to ensure availability for every output row.
        //       This requires all source tables to match or exceed the expected query output size.
        let context_sources_iter = source
            .tables
            .iter()
            .map(|table| (source.dataset.as_str(), table.as_str()));

        for (dataset, table) in context_sources_iter {
            let context_logger = logger.new(slog::o!(
                "context_dataset" => dataset.to_string(),
                "context_table" => table.to_string()
            ));
            debug!(context_logger, "Loading context schema");
            let schema_query = format!("SELECT * FROM {dataset}.{table}");
            let schema = match Self::resolve_schema(logger, amp_client, schema_query).await {
                Ok(schema) => schema,
                Err(e) => {
                    error!(context_logger, "Failed to load context schema";
                        "e" => ?e
                    );
                    continue;
                }
            };

            let record_batch = RecordBatch::new_empty(schema.clone().into());
            let mut columns = Vec::new();

            if need_block_hash_column {
                let Ok((block_hash_column, _)) = auto_block_hash_decoder(&record_batch) else {
                    debug!(
                        context_logger,
                        "Context schema does not contain block hash column, skipping"
                    );
                    continue;
                };

                columns.push(block_hash_column);
            }

            if need_block_timestamp_column {
                let Ok((block_timestamp_column, _)) = auto_block_timestamp_decoder(&record_batch)
                else {
                    debug!(
                        context_logger,
                        "Context schema does not contain block timestamp column, skipping"
                    );
                    continue;
                };

                columns.push(block_timestamp_column);
            }

            debug!(context_logger, "Creating context query");
            context_query = Some(ContextQuery::new(
                query,
                block_number_column,
                dataset,
                table,
                columns,
            ));
            break;
        }

        if let Some(context_query) = context_query {
            return Ok(BlockRangeQueryBuilder::new_with_context(context_query));
        }

        Err(Error::InvalidQuery(anyhow!(
            "query is required to output block numbers, block hashes and block timestamps"
        )))
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

fn validate_ident(s: &str) -> Result<(), Error> {
    if !regex_is_match!("^[a-zA-Z_][a-zA-Z0-9_-]{0,100}$", s) {
        return Err(Error::InvalidValue(
            anyhow!("invalid identifier '{s}': must start with a letter or an underscore, and contain only letters, numbers, hyphens, and underscores")
        ));
    }
    Ok(())
}

fn normalize_sql_ident(s: &str) -> String {
    match validate_ident(s) {
        Ok(()) => s.to_lowercase(),
        Err(_e) => sqlparser_latest::ast::Ident::with_quote('"', s).to_string(),
    }
}
