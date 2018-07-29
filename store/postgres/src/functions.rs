use diesel::sql_types::*;

// Create module for hosting the revert block stored procedure
sql_function! {
    revert_block,
    RevertBlock,
    (block_hash: Text)
}

// Create module for hosting the current setting stored procedure
sql_function! {
    current_setting,
    CurrentSetting,
    (setting_name: Text, missing_ok: Bool)
}

// Create module for hosting the set config stored procedure
sql_function! {
    set_config,
    SetConfig,
    (setting_name: Text, new_value: Text, is_local: Bool)
}
