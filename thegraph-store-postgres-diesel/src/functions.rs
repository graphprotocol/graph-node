use diesel::sql_types::*;

sql_function! {
    fn revert_blocks(block_hash: Text, block_hash: Text) -> Text
}