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
