pub mod raw;

use alloy::{
    json_abi::JsonAbi,
    primitives::{Address, BlockNumber},
};
use arrow::datatypes::Schema;

use crate::nozzle::{common::Ident, sql::Query};

/// Represents a valid data source of a Nozzle Subgraph.
///
/// This data source contains parsed, formatted, and resolved data.
#[derive(Debug, Clone)]
pub struct DataSource {
    /// The name of the data source.
    ///
    /// Used for observability to identify progress and errors produced by this data source.
    pub name: Ident,

    /// Contains the sources used by this data source.
    pub source: Source,

    /// Contains the transformations of source tables indexed by the Subgraph.
    pub transformer: Transformer,
}

impl DataSource {
    pub const KIND: &str = "nozzle";
}

/// Contains the sources that a data source uses.
#[derive(Debug, Clone)]
pub struct Source {
    /// The dataset from which SQL queries in the data source can query.
    pub dataset: Ident,

    /// The tables from which SQL queries in the data source can query.
    pub tables: Vec<Ident>,

    /// The contract address with which SQL queries in the data source interact.
    ///
    /// This address enables SQL query reuse through `sg_source_address()` calls instead of hard-coding the contract address.
    /// The `sg_source_address()` calls in SQL queries of the data source resolve to this contract address.
    ///
    /// SQL queries are not limited to using only this contract address.
    ///
    /// Defaults to an empty contract address.
    pub address: Address,

    /// The minimum block number that SQL queries in the data source can query.
    ///
    /// Defaults to the minimum possible block number.
    pub start_block: BlockNumber,

    /// The maximum block number that SQL queries in the data source can query.
    ///
    /// Defaults to the maximum possible block number.
    pub end_block: BlockNumber,
}

/// Contains the transformations of source tables indexed by the Subgraph.
#[derive(Debug, Clone)]
pub struct Transformer {
    /// The ABIs that SQL queries can reference to extract event signatures.
    ///
    /// The `sg_event_signature('CONTRACT_NAME', 'EVENT_NAME')` calls in the
    /// SQL queries resolve to a full event signature based on this list.
    pub abis: Vec<Abi>,

    /// The transformed tables that extract data from source tables for indexing.
    pub tables: Vec<Table>,
}

/// Represents an ABI of a smart contract.
#[derive(Debug, Clone)]
pub struct Abi {
    /// The name of the contract.
    pub name: Ident,

    /// The JSON ABI of the contract.
    pub contract: JsonAbi,
}

/// Represents a transformed table that extracts data from source tables for indexing.
#[derive(Debug, Clone)]
pub struct Table {
    /// The name of the transformed table.
    ///
    /// Must reference a valid entity name from the Subgraph schema.
    pub name: Ident,

    /// The SQL query that executes on the Nozzle server.
    ///
    /// The data resulting from this SQL query execution transforms into Subgraph entities.
    pub query: Query,

    /// The Arrow schema of this transformed table SQL query.
    ///
    /// This schema loads from the Nozzle server.
    pub schema: Schema,
}
