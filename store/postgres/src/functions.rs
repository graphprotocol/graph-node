use diesel::sql_types::*;

sql_function! {
    fn revert_blocks(block_hashes: Array<Text>) -> Text
}
