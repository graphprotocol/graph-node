use diesel::sql_types::{BigInt, Bool, Text};

// Create modules for hosting stored procedures
sql_function! { fn current_setting(setting_name: Text, missing_ok: Bool) }

sql_function! {
    fn set_config(setting_name: Text, new_value: Text, is_local: Bool)
}

sql_function! {
    fn attempt_chain_head_update(net_name: Text, ancestor_count: BigInt) -> Array<Text>
}

sql_function! {
    fn lookup_ancestor_block(start_block_hash: Text, ancestor_count: BigInt) -> Nullable<Jsonb>
}

sql_function! {
    fn pg_notify(channel: Text, msg: Text)
}
