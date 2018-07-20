use diesel::sql_types::*;

sql_function! {
    revert_block,
    RevertBlock,
    (block_hash: Text)
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
