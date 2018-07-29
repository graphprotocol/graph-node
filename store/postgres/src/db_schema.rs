table! {
    entities (id, subgraph, entity) {
        id -> Varchar,
        subgraph -> Varchar,
        entity -> Varchar,
        data -> Jsonb,
        event_source -> Varchar,
    }
}
