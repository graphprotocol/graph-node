//! Utilities for dealing with subgraph metadata

// Diesel tables for some of the metadata
// See also: ed42d219c6704a4aab57ce1ea66698e7
// Changes to the GraphQL schema might require changes to these tables
table! {
    subgraphs.dynamic_ethereum_contract_data_source (vid) {
        vid -> BigInt,
        id -> Text,
        kind -> Text,
        name -> Text,
        network -> Nullable<Text>,
        source -> Text,
        mapping -> Text,
        templates -> Nullable<Array<Text>>,
        ethereum_block_hash -> Binary,
        ethereum_block_number -> Numeric,
        deployment -> Text,
        block_range -> Range<Integer>,
    }
}
