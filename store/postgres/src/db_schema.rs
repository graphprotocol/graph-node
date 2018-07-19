table! {
    entities (id, data_source, entity) {
        id -> Varchar,
        data_source -> Varchar,
        entity -> Varchar,
        data -> Jsonb,
        event_source -> Varchar,
    }
}
