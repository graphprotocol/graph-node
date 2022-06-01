use std::collections::HashMap;
use std::fmt::Debug;

use protobuf::descriptor::FieldDescriptorProto;
use protobuf::descriptor::DescriptorProto;
use protobuf::descriptor::OneofDescriptorProto;
use protobuf::UnknownValueRef;
use protobuf::Message;
use std::convert::From;
use anyhow::Error;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct Field{
    pub name:String,
    pub type_name:String,
    pub required:bool,
    pub is_enum:bool,
    pub fields:Vec<Field>,

}

#[derive(Debug, Clone)]
pub struct PType{
    pub name:String,
    pub fields:Vec<Field>,
    pub descriptor:DescriptorProto
}

impl PType{
    pub fn has_req_fields(&self) -> bool{
        self.fields.iter().any(|f| f.required)
    }

    pub fn req_fields_as_string(&self) -> Option<String>{
        if self.has_req_fields(){
            Some(self.fields.iter()
                .filter(|f| f.required)
                .map(|f| f.name.clone())
                .collect::<Vec<String>>().join(",")
            )
        }else{
            None
        }
    }

    pub fn enum_fields_as_string(&self) -> Option<String>{

        if !self.fields.iter().any(|f| f.is_enum){
            return None;
        }

        Some(
            self.fields
            .iter()
            .filter(|f| f.is_enum)
            .map(|f| {
                let pairs = 
                    f.fields.iter()
                    .fold(String::new(), |mut acc, f|{
                        if !acc.is_empty(){
                            acc = acc + ","
                        }
                        acc + &f.name +": "+ &f.type_name
                    });

                format!("{}{{{}}}", f.name, pairs)
            })
            .collect::<Vec<String>>().join(",")
        )

    }

}


impl From<&FieldDescriptorProto> for Field {
    fn from(fd: &FieldDescriptorProto) -> Self {
        let options = fd.options.unknown_fields();


        Field {
            name: fd.name().to_owned(),
            type_name: fd.type_name().rsplit(".").next().unwrap().to_owned(),
            required: options.iter().find(|f| f.0 == 65001 && UnknownValueRef::Varint(0) == f.1 ).is_some(),
            is_enum: false,
            fields: vec![]
        }
    }
}

impl From<&OneofDescriptorProto> for Field {
    fn from(fd: &OneofDescriptorProto) -> Self {
        Field {
            name: fd.name().to_owned(),
            type_name: "".to_owned(),
            required: false,
            is_enum: true,
            fields: vec![]

        }
    }
}


impl From<&DescriptorProto> for PType {
    fn from(dp: &DescriptorProto) -> Self {

        let mut fields = dp.oneof_decl.iter()
        .enumerate()
        .map(|(index, fd)| {
            let mut fld = Field::from(fd);

            fld.fields = 
                dp.field.iter()
                    .filter(|fd| fd.oneof_index.is_some())
                    .filter(|fd| *fd.oneof_index.as_ref().unwrap() as usize ==  index)
                    .map(|fd| Field::from(fd)).collect::<Vec<Field>>();

            fld
        }).collect::<Vec<Field>>();


        fields.extend(
            dp.field.iter()
                .filter(|fd| fd.oneof_index.is_none())
                .map(|fd| Field::from(fd)).collect::<Vec<Field>>()
        );


        PType {
            name: dp.name().to_owned(),
            fields: fields,
            descriptor: dp.clone()
        }
    }
}




pub fn parse_proto_file<'a, P>(file_path: P) -> Result<HashMap<String, PType>, Error>
where
    P: 'a + AsRef<Path> + Debug,
{
    let dir = file_path.as_ref().parent().unwrap();

    let fd = protobuf_parse::Parser::new()
        .include(dir)
        .input(&file_path)
        .file_descriptor_set()?;

    assert!(fd.file.len() == 1);
    assert!(fd.file[0].has_name());

    let file_name = file_path.as_ref().clone().file_name().unwrap().to_str().unwrap();
    assert!(fd.file[0].name() == file_name);

    let ret_val = fd
        .file
        .iter() //should be just 1 file
        .flat_map(|f| f.message_type.iter())
        .map(|dp| { //this is message type
           
            /*
             ----- cosmos/codec.proto
             not sure what do we do with oneof
            message Evidence {
                oneof sum {
                  DuplicateVoteEvidence     duplicate_vote_evidence       = 1;
                  LightClientAttackEvidence light_client_attack_evidence  = 2;
                }
            }

            pub struct Evidence {
                #[prost(oneof="evidence::Sum", tags="1, 2")]
                pub sum: ::core::option::Option<evidence::Sum>,
            }


            let is_enum = dp.oneof_decl.len() > 0;

            oneof_decl: [
                    OneofDescriptorProto {
                        name: Some(
                            "sum",
                        ),
                        options: MessageField(
                            None,
                        ),
                        special_fields: SpecialFields {
                            unknown_fields: UnknownFields {
                                fields: None,
                            },
                            cached_size: CachedSize {
                                size: 0,
                            },
                        },
                    },
                ],

            */  

            (
                dp.name().to_owned(),
                PType::from(dp)
            )
        })
        .collect::<HashMap<String, PType>>();

    Ok(ret_val)
}
