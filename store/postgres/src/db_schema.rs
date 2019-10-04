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
    large_notifications(id) {
        id -> Integer,
        payload -> Text,
        created_at -> Timestamp,
    }
}

table! {
    ens_names(hash) {
        hash -> Varchar,
        name -> Varchar,
    }
}

table! {
    /// `id` is the hash of contract address + encoded function call + block number.
    eth_call_cache (id) {
        id -> Bytea,
        return_value -> Bytea,
        contract_address -> Bytea,
        block_number -> Integer,
    }
}

table! {
    /// When was a cached call on a contract last used? This is useful to clean old data.
    eth_call_meta (contract_address) {
        contract_address -> Bytea,
        accessed_at -> Date,
    }
}

joinable!(eth_call_cache -> eth_call_meta (contract_address));
allow_tables_to_appear_in_same_query!(eth_call_cache, eth_call_meta);
