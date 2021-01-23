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
