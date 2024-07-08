use diesel::sql_types::{Binary, Bool, Integer, Nullable, Numeric, Range, Text};

// Create modules for hosting stored procedures
define_sql_function! { fn current_setting(setting_name: Text, missing_ok: Bool) }

define_sql_function! {
    fn set_config(setting_name: Text, new_value: Text, is_local: Bool)
}

define_sql_function! {
    fn lower(range: Range<Integer>) -> Integer
}

define_sql_function! {
    #[sql_name="coalesce"]
    fn coalesce_numeric(first: Nullable<Numeric>, second: Nullable<Numeric>) -> Nullable<Numeric>
}

define_sql_function! {
    #[sql_name="coalesce"]
    fn coalesce_binary(first: Nullable<Binary>, second: Nullable<Binary>) -> Nullable<Binary>
}
