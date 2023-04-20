pub fn is_connection_type(field_or_type_name: &str) -> bool {
    field_or_type_name.ends_with("Connection")
}

#[test]
fn test_is_valid_connection_type() {
    assert!(is_connection_type("FooConnection"));
    assert!(!is_connection_type("Foo"));
}
