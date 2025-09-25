mod filter_blocks;
mod resolve_event_signatures;
mod resolve_source_address;
mod validate_tables;

use std::fmt;

use alloy::{
    json_abi::JsonAbi,
    primitives::{Address, BlockNumber},
};
use anyhow::{bail, Context, Result};
use itertools::Itertools;
use sqlparser_latest::ast;

/// Represents a valid SQL query of a Nozzle Subgraph.
///
/// Parses, validates and resolves a SQL query and prepares it for execution on a Nozzle server.
/// The data returned by executing this query is used to create Subgraph entities.
pub struct Query {
    /// The raw SQL AST that represents the SQL query.
    ast: ast::Query,

    /// The dataset that the SQL query requests data from.
    dataset: format::Ident,

    /// The tables that the SQL query requests data from.
    tables: Vec<format::Ident>,
}

/// Contains the ABI information that is used to resolve event signatures in SQL queries.
pub struct Abi<'a> {
    /// The name of the contract.
    pub name: &'a str,

    /// The JSON ABI of the contract.
    pub contract: &'a JsonAbi,
}

impl Query {
    /// Parses, validates and resolves a SQL query and prepares it for execution on a Nozzle server.
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
        sql: impl AsRef<str>,
        dataset: impl AsRef<str>,
        tables: impl IntoIterator<Item = impl AsRef<str>>,
        source_address: &Address,
        abis: impl IntoIterator<Item = Abi<'a>>,
    ) -> Result<Self> {
        let mut query = parse::query(sql).context("failed to parse SQL query")?;
        let dataset = format::Ident::new(dataset);
        let tables = tables.into_iter().map(format::Ident::new).collect_vec();
        let abis = abis.into_iter().collect_vec();

        Self::validate(&query, &dataset, &tables).context("failed to validate SQL query")?;
        Self::resolve(&mut query, source_address, &abis).context("failed to resolve SQL query")?;

        Ok(Self {
            ast: query,
            dataset,
            tables,
        })
    }

    /// Applies a block range filter to this SQL query.
    ///
    /// Creates temporary ordered result sets for each table in the dataset, limiting
    /// the blocks processed during execution.
    ///
    /// The temporary result sets replace the tables referenced in this SQL query.
    ///
    /// This ensures deterministic output during query execution and enables resuming
    /// after failures or when new blocks are available.
    pub fn filter_blocks(&mut self, start_block: BlockNumber, end_block: BlockNumber) {
        filter_blocks::filter_blocks(
            &mut self.ast,
            &self.dataset,
            &self.tables,
            start_block,
            end_block,
        );
    }

    /// Validates the SQL query.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The SQL query references unknown tables and datasets
    /// - The SQL query uses custom `SETTINGS`
    ///
    /// The returned error is deterministic.
    fn validate(
        query: &ast::Query,
        dataset: &format::Ident,
        tables: &[format::Ident],
    ) -> Result<()> {
        validate_tables::validate_tables(query, dataset, tables)?;

        if query.settings.is_some() {
            bail!("custom SETTINGS are not allowed");
        }

        Ok(())
    }

    /// Resolves Subgraph-specific function calls in the SQL query.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Source address function calls cannot be resolved
    /// - Event signature function calls cannot be resolved
    ///
    /// The returned error is deterministic.
    fn resolve(query: &mut ast::Query, source_address: &Address, abis: &[Abi<'_>]) -> Result<()> {
        resolve_source_address::resolve_source_address(query, source_address)?;
        resolve_event_signatures::resolve_event_signatures(query, abis)?;

        Ok(())
    }
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.ast)
    }
}

mod format {
    use std::fmt;

    /// Represents a normalized SQL identifier.
    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub(super) struct Ident(Box<str>);

    impl Ident {
        /// Creates a normalized SQL identifier.
        pub(super) fn new(s: impl AsRef<str>) -> Self {
            Self(s.as_ref().to_lowercase().into())
        }
    }

    impl fmt::Display for Ident {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }
}

mod parse {
    use anyhow::{anyhow, bail, Context, Result};
    use itertools::Itertools;
    use sqlparser_latest::{ast, dialect::GenericDialect, parser::Parser};

    /// Parses a SQL query and returns its AST.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The SQL query cannot be parsed
    /// - The SQL query has multiple SQL statements
    /// - The SQL query is not a `SELECT` query
    pub(super) fn query(s: impl AsRef<str>) -> Result<ast::Query> {
        let statement = Parser::parse_sql(&GenericDialect {}, s.as_ref())
            .context("invalid SQL query")?
            .into_iter()
            .exactly_one()
            .map_err(|e| anyhow!("expected exactly one SQL statement, found {}", e.count()))?;

        let query = match statement {
            ast::Statement::Query(query) => *query,
            _ => bail!("invalid SQL query: only SELECT statements are allowed"),
        };

        Ok(query)
    }
}
