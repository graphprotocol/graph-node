const PROTO_FILE: &str = "tests/resources/cosmos/codec.proto";

mod utils;
use common::*;

#[test]
fn check_type_count() {
    let types = parse_proto_file(PROTO_FILE).expect("Unable to read proto file!");
    assert_eq!(41, types.len());
}

#[test]
fn check_block_type_field_count() {
    let types = parse_proto_file(PROTO_FILE).expect("Unable to read proto file!");
    let block = types.get("Block");
    assert!(block.is_some());

    let ptype = block.unwrap();
    assert_eq!(4, ptype.fields.len());

    ptype.fields.iter().for_each(|f| {
        match f.name.as_ref() {
            "header" => assert!(f.required, "Block.header field should be required!"),
            "data" => assert!(f.required, "Block.data field should be required!"),
            "evidence" => assert!(f.required, "Block.evidence field should be required!"),
            "last_commit" => assert!(
                !f.required,
                "Block.last_commit field should NOT be required!"
            ),
            _ => assert!(false, "Unexpected message field [{}]!", f.name),
        };
    });
}

#[test]
fn check_block_type_req_fld_string() {
    let types = parse_proto_file(PROTO_FILE).expect("Unable to read proto file!");
    let block = types.get("Block");
    assert!(block.is_some());

    let ptype = block.unwrap();

    assert!(ptype.has_req_fields());
    assert_eq!(
        "__required__{header: Header,data: Data,evidence: EvidenceList}",
        ptype.req_fields_as_string().unwrap()
    );
}

#[test]
fn check_eventdata_type_has_no_req_flds() {
    let types = parse_proto_file(PROTO_FILE).expect("Unable to read proto file!");
    let block = types.get("EventData");
    assert!(block.is_some());

    let ptype = block.unwrap();

    assert!(!ptype.has_req_fields());

    assert_eq!(2, ptype.fields.len());
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
