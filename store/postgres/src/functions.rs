use diesel::sql_types::*;

// Create modules for hosting stored procedures
sql_function! {
    revert_block,
    RevertBlock,
    (block_hash: Text, subgraph: Text)
}
sql_function! {
    current_setting,
    CurrentSetting,
    (setting_name: Text, missing_ok: Bool)
}
sql_function! {
    set_config,
    SetConfig,
    (setting_name: Text, new_value: Text, is_local: Bool)
}
sql_function! {
    attempt_head_update,
    AttemptHeadUpdate,
    (net_name: Varchar, ancestor_count: BigInt) -> Array<Varchar>
}
sql_function! {
    lookup_ancestor_block,
    LookupAncestorBlock,
    (start_block_hash: Varchar, ancestor_count: BigInt) -> Nullable<Jsonb>
}
