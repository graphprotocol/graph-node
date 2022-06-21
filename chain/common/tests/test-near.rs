use common::*;
mod utils;

const PROTO_FILE: &str = "tests/resources/near/codec.proto";

#[test]
fn check_type_count() {
    let types = parse_proto_file(PROTO_FILE).expect("Unable to read proto file!");
    assert_eq!(62, types.len());
}

#[test]
fn can_read_proto_file_ok() {
    let types = parse_proto_file(PROTO_FILE);
    assert!(types.is_ok());
}
#[test]
fn can_read_proto_file_should_fail() {
    let types = parse_proto_file(utils::BOGUS_FILE);
    assert!(types.is_err());
}
