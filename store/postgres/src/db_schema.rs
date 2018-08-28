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
