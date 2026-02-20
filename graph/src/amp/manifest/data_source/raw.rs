use std::{collections::HashSet, sync::LazyLock};

use alloy::{
    json_abi::JsonAbi,
    primitives::{Address, BlockNumber},
};
use anyhow::anyhow;
use arrow::{array::RecordBatch, datatypes::Schema};
use futures03::future::try_join_all;
use itertools::Itertools;
use semver::Version;
use serde::Deserialize;
use slog::{debug, Logger};
use thiserror::Error;

use super::{Abi, DataSource, Source, Table, Transformer};
use crate::{
    amp::{
        self,
        codec::utils::{
            auto_block_hash_decoder, auto_block_number_decoder, auto_block_timestamp_decoder,
        },
        error::IsDeterministic,
        sql::{
            normalize_sql_ident, validate_ident, BlockRangeQueryBuilder, ContextQuery, ValidQuery,
        },
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
        amp_context: Option<(String, String)>,
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

        validate_ident(&name)
            .map_err(|e| Error::InvalidValue(e).source_context("invalid `name`"))?;
        Self::validate_kind(kind)?;

        let source = source
            .resolve()
            .map_err(|e| e.source_context("invalid `source`"))?;

        let transformer = transformer
            .resolve(
                &logger,
                link_resolver,
                amp_client,
                input_schema,
                &source,
                amp_context,
            )
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
        amp_context: Option<(String, String)>,
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
            amp_context,
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
        amp_context: Option<(String, String)>,
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

        let table_futs = tables.into_iter().enumerate().map(|(i, table)| {
            let amp_context = amp_context.clone();
            async move {
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
                        amp_context,
                    )
                    .await
                    .map_err(|e| e.source_context(format!("invalid `tables` at index {i}")))
            }
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

        validate_ident(&name)
            .map_err(|e| Error::InvalidValue(e).source_context("invalid `name`"))?;
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
        amp_context: Option<(String, String)>,
    ) -> Result<Table, Error> {
        let Self { name, query, file } = self;

        validate_ident(&name)
            .map_err(|e| Error::InvalidValue(e).source_context("invalid `name`"))?;
        let query = match Self::resolve_query(query, source, abis)? {
            Some(query) => query,
            None => Self::resolve_file(logger, link_resolver, file, source, abis).await?,
        };

        debug!(logger, "Resolving query schema");
        let schema = Self::resolve_schema(logger, amp_client, &query).await?;

        for field in schema.fields() {
            validate_ident(field.name()).map_err(|e| {
                Error::InvalidValue(e).source_context(format!(
                    "invalid query output schema: invalid column '{}'",
                    field.name()
                ))
            })?;
        }

        let block_range_query_builder = Self::resolve_block_range_query_builder(
            logger,
            amp_client,
            input_schema,
            query,
            schema.clone(),
            amp_context,
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
        query: ValidQuery,
        schema: Schema,
        amp_context: Option<(String, String)>,
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

        let (context_dataset, context_table) = amp_context.ok_or_else(|| {
            Error::InvalidQuery(anyhow!(
                "query requires context columns (block hash/timestamp) but no Amp context config is available"
            ))
        })?;

        debug!(logger, "Resolving context query";
            "context_dataset" => &context_dataset,
            "context_table" => &context_table
        );

        let schema_query = format!("SELECT * FROM {context_dataset}.{context_table}");
        let context_schema = Self::resolve_schema(logger, amp_client, schema_query).await?;

        let context_record_batch = RecordBatch::new_empty(context_schema.into());
        let mut columns = Vec::new();

        if need_block_hash_column {
            let (block_hash_column, _) =
                auto_block_hash_decoder(&context_record_batch).map_err(|_| {
                    Error::InvalidQuery(anyhow!(
                        "context table '{context_dataset}.{context_table}' does not contain a block hash column"
                    ))
                })?;
            columns.push(block_hash_column);
        }

        if need_block_timestamp_column {
            let (block_timestamp_column, _) =
                auto_block_timestamp_decoder(&context_record_batch).map_err(|_| {
                    Error::InvalidQuery(anyhow!(
                        "context table '{context_dataset}.{context_table}' does not contain a block timestamp column"
                    ))
                })?;
            columns.push(block_timestamp_column);
        }

        let context_query = ContextQuery::new(
            query,
            block_number_column,
            &context_dataset,
            &context_table,
            columns,
        );

        Ok(BlockRangeQueryBuilder::new_with_context(context_query))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::amp::error::IsDeterministic;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use futures03::future::BoxFuture;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Debug, thiserror::Error)]
    #[error("mock error: schema not found for query")]
    struct MockError;

    impl IsDeterministic for MockError {
        fn is_deterministic(&self) -> bool {
            true
        }
    }

    /// A mock Amp client that returns pre-configured schemas keyed by query string.
    struct MockAmpClient {
        schemas: Mutex<HashMap<String, Schema>>,
    }

    impl MockAmpClient {
        fn new() -> Self {
            Self {
                schemas: Mutex::new(HashMap::new()),
            }
        }

        fn add_schema(&self, query: &str, schema: Schema) {
            self.schemas
                .lock()
                .unwrap()
                .insert(query.to_string(), schema);
        }
    }

    impl amp::Client for MockAmpClient {
        type Error = MockError;

        fn schema(
            &self,
            _logger: &slog::Logger,
            query: impl ToString,
        ) -> BoxFuture<'static, Result<Schema, Self::Error>> {
            let query_str = query.to_string();
            let schema = self.schemas.lock().unwrap().get(&query_str).cloned();
            Box::pin(async move { schema.ok_or(MockError) })
        }

        fn query(
            &self,
            _logger: &slog::Logger,
            _query: impl ToString,
            _request_metadata: Option<amp::client::RequestMetadata>,
        ) -> futures03::stream::BoxStream<'static, Result<amp::client::ResponseBatch, Self::Error>>
        {
            Box::pin(futures03::stream::empty())
        }
    }

    fn test_logger() -> slog::Logger {
        slog::Logger::root(slog::Discard, slog::o!())
    }

    fn schema_without_hash() -> Schema {
        Schema::new(vec![
            Field::new("_block_num", DataType::UInt64, false),
            Field::new("value", DataType::Utf8, true),
        ])
    }

    fn schema_with_all_columns() -> Schema {
        Schema::new(vec![
            Field::new("_block_num", DataType::UInt64, false),
            Field::new("block_hash", DataType::FixedSizeBinary(32), false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("value", DataType::Utf8, true),
        ])
    }

    fn context_schema_with_hash() -> Schema {
        Schema::new(vec![
            Field::new("_block_num", DataType::UInt64, false),
            Field::new("block_hash", DataType::FixedSizeBinary(32), false),
        ])
    }

    fn make_valid_query(sql: &str) -> ValidQuery {
        ValidQuery::new(
            sql,
            "my_dataset",
            ["my_table"].iter().copied(),
            &alloy::primitives::Address::ZERO,
            std::iter::empty::<(&str, &alloy::json_abi::JsonAbi)>(),
        )
        .unwrap()
    }

    /// When a query lacks block hash/timestamp columns, the context CTE uses
    /// context_dataset and context_table from config.
    #[tokio::test]
    async fn context_query_uses_config() {
        let logger = test_logger();
        let client = MockAmpClient::new();

        let main_query = "SELECT _block_num, value FROM my_dataset.my_table";
        client.add_schema(main_query, schema_without_hash());
        client.add_schema(
            "SELECT * FROM ctx_dataset.ctx_blocks",
            context_schema_with_hash(),
        );

        let valid_query = make_valid_query(main_query);

        let result = RawTable::resolve_block_range_query_builder(
            &logger,
            &client,
            None,
            valid_query,
            schema_without_hash(),
            Some(("ctx_dataset".to_string(), "ctx_blocks".to_string())),
        )
        .await;

        assert!(
            result.is_ok(),
            "Expected success when config provides context; got: {:?}",
            result.err()
        );
    }

    /// The old source.tables iteration is replaced — config values are the sole
    /// source of context dataset and table. When no config is provided and context
    /// columns are needed, resolution fails.
    #[tokio::test]
    async fn context_query_always_has_config() {
        let logger = test_logger();
        let client = MockAmpClient::new();

        let main_query = "SELECT _block_num, value FROM my_dataset.my_table";
        client.add_schema(main_query, schema_without_hash());

        let valid_query = make_valid_query(main_query);

        let result = RawTable::resolve_block_range_query_builder(
            &logger,
            &client,
            None,
            valid_query,
            schema_without_hash(),
            None,
        )
        .await;

        assert!(
            result.is_err(),
            "Expected error when amp_context is None and context columns are needed"
        );
        let err_msg = format!("{:#}", result.unwrap_err());
        assert!(
            err_msg.contains("no Amp context config"),
            "Error should mention missing config; got: {err_msg}"
        );
    }

    /// When the query already includes block hash and timestamp columns,
    /// context config is not needed and resolution succeeds.
    #[tokio::test]
    async fn context_query_not_needed_when_columns_present() {
        let logger = test_logger();
        let client = MockAmpClient::new();

        let main_query = "SELECT _block_num, block_hash, timestamp, value FROM my_dataset.my_table";
        client.add_schema(main_query, schema_with_all_columns());

        let valid_query = make_valid_query(main_query);

        let result = RawTable::resolve_block_range_query_builder(
            &logger,
            &client,
            None,
            valid_query,
            schema_with_all_columns(),
            None,
        )
        .await;

        assert!(
            result.is_ok(),
            "Expected success when all columns present; got: {:?}",
            result.err()
        );
    }

    /// AmpChainConfig context fields are threaded through the full resolution
    /// chain (RawDataSource → RawTransformer → RawTable).
    #[tokio::test]
    async fn context_config_threaded_through_resolution() {
        let logger = test_logger();
        let client = MockAmpClient::new();

        let main_query = "SELECT _block_num, value FROM my_dataset.my_table";
        client.add_schema(main_query, schema_without_hash());
        client.add_schema(
            "SELECT * FROM ctx_dataset.ctx_blocks",
            context_schema_with_hash(),
        );

        let link_resolver = crate::components::link_resolver::FileLinkResolver::default();

        let raw_data_source = RawDataSource {
            name: "test_ds".to_string(),
            kind: "amp".to_string(),
            network: "mainnet".to_string(),
            source: RawSource {
                dataset: "my_dataset".to_string(),
                tables: vec!["my_table".to_string()],
                address: None,
                start_block: None,
                end_block: None,
            },
            transformer: RawTransformer {
                api_version: semver::Version::new(0, 0, 1),
                abis: None,
                tables: vec![RawTable {
                    name: "TestEntity".to_string(),
                    query: Some(main_query.to_string()),
                    file: None,
                }],
            },
        };

        let result = raw_data_source
            .resolve(
                &logger,
                &link_resolver,
                &client,
                None,
                Some(("ctx_dataset".to_string(), "ctx_blocks".to_string())),
            )
            .await;

        assert!(
            result.is_ok(),
            "Expected successful resolution with threaded context config; got: {:?}",
            result.err()
        );
    }
}
