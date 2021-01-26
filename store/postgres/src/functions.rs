use diesel::sql_types::{Bool, Text};

// Create modules for hosting stored procedures
sql_function! { fn current_setting(setting_name: Text, missing_ok: Bool) }

sql_function! {
    fn set_config(setting_name: Text, new_value: Text, is_local: Bool)
}

sql_function! {
    fn pg_notify(channel: Text, msg: Text)
}
