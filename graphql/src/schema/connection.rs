pub fn is_connection_type(field_or_type_name: &str) -> bool {
    field_or_type_name.ends_with("Connection")
}
