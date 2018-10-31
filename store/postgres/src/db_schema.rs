table! {
    entities (id, subgraph, entity) {
        id -> Varchar,
        subgraph -> Varchar,
        entity -> Varchar,
        data -> Jsonb,
        event_source -> Varchar,
    }
}

table! {
    ethereum_networks (name) {
        name -> Varchar,
        head_block_hash -> Nullable<Varchar>,
        head_block_number -> Nullable<BigInt>,
        net_version -> Nullable<Varchar>,
        genesis_block_hash -> Nullable<Varchar>,
    }
}

table! {
    ethereum_blocks (hash) {
        hash -> Varchar,
        number -> BigInt,
        parent_hash -> Nullable<Varchar>,
        network_name -> Varchar, // REFERENCES ethereum_networks (name),
        data -> Jsonb,
    }
}

table! {
    subgraphs (id) {
        id -> Varchar,
        network_name -> Varchar,
        latest_block_hash -> Varchar,
        latest_block_number -> BigInt,
    }
}
allow_tables_to_appear_in_same_query!(entities, subgraphs);
joinable!(entities -> subgraphs (subgraph));

table! {
    subgraph_names (subgraph_name) {
        subgraph_name -> Varchar,
        subgraph_id -> Nullable<Varchar>,
        access_token -> Nullable<Varchar>,
    }
}
