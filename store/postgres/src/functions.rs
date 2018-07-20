use diesel::sql_types::*;

sql_function! {
    fn revert_block_group(block_hashes: Array<Text>);
}

sql_function! {
    fn current_setting(setting_name: Text) -> Text;
}

sql_function! {
    fn set_config(setting_name: Text, new_value: Text, is_local: Bool);
}
