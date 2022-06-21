const PROTO_FILE:&str = "tests/resources/cosmos/codec.proto";

mod utils;
use common::*;

#[test]
fn check_eventlist_type_field_count(){
  let types = parse_proto_file(PROTO_FILE).expect("Unable to read proto file!");
  //let block = types.get("PublicKey");
  let block = types.get("ModeInfo");
 
  //let block = types.get("EventTx");
  
  assert!(block.is_some());

  let ptype = block.unwrap();

  println!("{:#?}", ptype);

  //println!("{}", ptype.enum_fields_as_string().unwrap());


  //println!("{:#?}", ptype.descriptor);

/*
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Evidence {
    #[prost(oneof="evidence::Sum", tags="1, 2")]
    pub sum: ::core::option::Option<evidence::Sum>,
}

/// Nested message and enum types in `Evidence`.
pub mod evidence {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Sum {
        #[prost(message, tag="1")]
        DuplicateVoteEvidence(super::DuplicateVoteEvidence),
        #[prost(message, tag="2")]
        LightClientAttackEvidence(super::LightClientAttackEvidence),
    }
}

pub struct DuplicateVoteEvidence {
    #[prost(message, optional, tag="1")]
    pub vote_a: ::core::option::Option<EventVote>,
    #[prost(message, optional, tag="2")]
    pub vote_b: ::core::option::Option<EventVote>,
    #[prost(int64, tag="3")]
    pub total_voting_power: i64,
    #[prost(int64, tag="4")]
    pub validator_power: i64,
    #[prost(message, optional, tag="5")]
    pub timestamp: ::core::option::Option<Timestamp>,
}

pub struct LightClientAttackEvidence {
    #[prost(message, optional, tag="1")]
    pub conflicting_block: ::core::option::Option<LightBlock>,
    #[prost(int64, tag="2")]
    pub common_height: i64,
    #[prost(message, repeated, tag="3")]
    pub byzantine_validators: ::prost::alloc::vec::Vec<Validator>,
    #[prost(int64, tag="4")]
    pub total_voting_power: i64,
    #[prost(message, optional, tag="5")]
    pub timestamp: ::core::option::Option<Timestamp>,
}

*/



  //assert_eq!(4, ptype.fields.len());

  // ptype.fields.iter().for_each(|f|{
  //   match f.name.as_ref(){
  //     "header" => assert!(f.required, "Block.header field should be required!"),
  //     "data" => assert!(f.required, "Block.data field should be required!"),
  //     "evidence" => assert!(f.required, "Block.evidence field should be required!"),
  //     "last_commit" => assert!(!f.required, "Block.last_commit field should NOT be required!"),
  //     _ =>  assert!(false, "Unexpected message field [{}]!", f.name)
  //   };
  // });
}




#[test]
fn check_type_count(){
    let types = parse_proto_file(PROTO_FILE).expect("Unable to read proto file!");
    assert_eq!(37,types.len());
}

#[test]
fn check_block_type_field_count(){
  let types = parse_proto_file(PROTO_FILE).expect("Unable to read proto file!");
  let block = types.get("Block");
  assert!(block.is_some());

  let ptype = block.unwrap();
  assert_eq!(4, ptype.fields.len());

  ptype.fields.iter().for_each(|f|{
    match f.name.as_ref(){
      "header" => assert!(f.required, "Block.header field should be required!"),
      "data" => assert!(f.required, "Block.data field should be required!"),
      "evidence" => assert!(f.required, "Block.evidence field should be required!"),
      "last_commit" => assert!(!f.required, "Block.last_commit field should NOT be required!"),
      _ =>  assert!(false, "Unexpected message field [{}]!", f.name)
    };
  });
}

#[test]
fn check_block_type_req_fld_string(){
  let types = parse_proto_file(PROTO_FILE).expect("Unable to read proto file!");
  let block = types.get("Block");
  assert!(block.is_some());

  let ptype = block.unwrap();

  assert!(ptype.has_req_fields());
  assert_eq!("header,data,evidence", ptype.req_fields_as_string().unwrap());


}

#[test]
fn check_eventdata_type_has_no_req_flds(){
  let types = parse_proto_file(PROTO_FILE).expect("Unable to read proto file!");
  let block = types.get("EventData");
  assert!(block.is_some());

  let ptype = block.unwrap();

  assert!(!ptype.has_req_fields());

  assert_eq!(2, ptype.fields.len());

}



#[test]
fn can_read_proto_file_ok(){
    let types = parse_proto_file(PROTO_FILE);
    assert!(types.is_ok());
}
#[test]
fn can_read_proto_file_should_fail(){
    let types = parse_proto_file(utils::BOGUS_FILE);
    assert!(types.is_err());
}
