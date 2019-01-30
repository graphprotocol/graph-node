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
    entity_history (id) {
        id -> Integer,
        event_id -> BigInt,
        entity_id -> Varchar,
        subgraph -> Varchar,
        entity -> Varchar,
        data_before -> Nullable<Jsonb>,
        data_after -> Nullable<Jsonb>,
        reversion -> Bool,
    }
}

table! {
    event_meta_data (id) {
        id -> Integer,
        db_transaction_id -> BigInt,
        db_transaction_time -> Timestamp,
        op_id -> SmallInt,
        source -> Nullable<Varchar>,
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
