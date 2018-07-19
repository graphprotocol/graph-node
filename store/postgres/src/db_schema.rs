table! {
    entities (id, data_source, entity) {
        id -> Varchar,
        data_source -> Varchar,
        entity -> Varchar,
        data -> Jsonb,
        latest_block_hash -> Varchar,
    }
}
