use diesel::sql_types::*;

sql_function! {
    fn revert_block_group(block_hashes: Array<Text>);
}
