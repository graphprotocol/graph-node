mod block_range_query;
mod context_query;
mod event_signature_resolver;
mod parser;
mod source_address_resolver;
mod table_extractor;
mod table_validator;

use std::{fmt, ops::RangeInclusive};

use alloy::{
    json_abi::JsonAbi,
    primitives::{Address, BlockNumber},
};
use anyhow::{bail, Context, Result};
use itertools::Itertools;
use sqlparser_latest::ast;

use self::{
    block_range_query::new_block_range_query,
    context_query::new_context_query,
    event_signature_resolver::resolve_event_signatures,
    parser::parse_query,
    source_address_resolver::resolve_source_address,
    table_extractor::{extract_tables, normalize_table},
    table_validator::validate_tables,
};

/// Represents a valid SQL query that can be executed on an Amp server.
#[derive(Debug, Clone)]
pub struct ValidQuery {
    query: ast::Query,
}

impl ValidQuery {
    /// Parses, validates and resolves the input SQL query.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The SQL query cannot be parsed
    /// - The SQL query is not valid
    /// - The SQL query cannot be resolved
    ///
    /// The returned error is deterministic.
    pub fn new<'a>(
        sql: &str,
        dataset: &str,
        tables: impl IntoIterator<Item = &'a str>,
        source_address: &Address,
        abis: impl IntoIterator<Item = (&'a str, &'a JsonAbi)>,
    ) -> Result<Self> {
        let mut query = parse_query(sql).context("failed to parse SQL query")?;

        Self::validate(&query, dataset, tables).context("failed to validate SQL query")?;
        Self::resolve(&mut query, source_address, abis).context("failed to resolve SQL query")?;

        Ok(Self { query })
    }

    /// Validates the SQL query.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The SQL query references unknown datasets or tables
    /// - The SQL query uses custom `SETTINGS`
    ///
    /// The returned error is deterministic.
    fn validate<'a>(
        query: &ast::Query,
        dataset: &str,
        tables: impl IntoIterator<Item = &'a str>,
    ) -> Result<()> {
        validate_tables(query, dataset, tables)?;

        if query.settings.is_some() {
            bail!("custom SETTINGS are not allowed");
        }

        Ok(())
    }

    /// Resolves subgraph-specific function calls in the SQL query.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Source address function calls cannot be resolved
    /// - Event signature function calls cannot be resolved
    ///
    /// The returned error is deterministic.
    fn resolve<'a>(
        query: &mut ast::Query,
        source_address: &Address,
        abis: impl IntoIterator<Item = (&'a str, &'a JsonAbi)>,
    ) -> Result<()> {
        resolve_source_address(query, source_address)?;
        resolve_event_signatures(query, &abis.into_iter().collect_vec())?;

        Ok(())
    }
}

impl fmt::Display for ValidQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.query)
    }
}

/// Represents a valid SQL query that contains columns required by Amp subgraphs.
#[derive(Debug, Clone)]
pub struct ContextQuery {
    query: ast::Query,
    block_number_column: String,
}

impl ContextQuery {
    /// Wraps the SQL query with additional context columns from a separate dataset.
    ///
    /// Creates two CTEs: one wrapping the input `query` and another loading context columns
    /// from the specified context dataset and table. Joins both CTEs on block numbers to
    /// include the context columns in the original query's output.
    ///
    /// This enables including columns required by Amp subgraphs in the original SQL query.
    pub fn new<'a>(
        valid_query: ValidQuery,
        block_number_column: &str,
        context_dataset: &str,
        context_table: &str,
        context_columns: impl IntoIterator<Item = &'a str>,
    ) -> Self {
        let ValidQuery { query } = valid_query;

        let query = new_context_query(
            &query,
            block_number_column,
            context_dataset,
            context_table,
            context_columns,
        );

        Self {
            query,
            block_number_column: block_number_column.to_string(),
        }
    }
}

/// Builds valid SQL queries for execution on an Amp server with block range limits.
#[derive(Debug, Clone)]
pub struct BlockRangeQueryBuilder {
    query: ast::Query,
    block_number_column: String,
}

impl BlockRangeQueryBuilder {
    /// Creates a new block range query builder with the specified valid SQL query.
    pub fn new(valid_query: ValidQuery, block_number_column: &str) -> Self {
        let ValidQuery { query } = valid_query;

        Self {
            query,
            block_number_column: block_number_column.to_string(),
        }
    }

    /// Creates a new block range query builder with the specified context SQL query.
    pub fn new_with_context(context_query: ContextQuery) -> Self {
        let ContextQuery {
            query,
            block_number_column,
        } = context_query;

        Self {
            query,
            block_number_column,
        }
    }

    /// Limits the query execution to the specified block range.
    ///
    /// Wraps this SQL query in a CTE, and creates CTEs for every table it references.
    /// These CTEs load data from the referenced tables only on the specified block range.
    /// All the table references in the original SQL query are replaced with the created CTE names.
    ///
    /// The output is ordered by block numbers.
    pub fn build_with_block_range(&self, block_range: &RangeInclusive<BlockNumber>) -> String {
        new_block_range_query(&self.query, &self.block_number_column, block_range).to_string()
    }
}
