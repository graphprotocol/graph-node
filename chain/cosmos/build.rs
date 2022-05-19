// fn main() {
//     println!("cargo:rerun-if-changed=proto");
//     tonic_build::configure()
//         .out_dir("src/protobuf")
//         .compile(&["proto/type.proto"], &["proto"])
//         .expect("Failed to compile Firehose Cosmos proto(s)");
// }
const PROT_FILE: &str = "proto/type.proto";

fn main() {

     //println!("cargo:rerun-if-changed=build.rs");
     println!("cargo:rerun-if-changed=proto");

     let types = common::parse_proto_file(PROT_FILE).expect("Unable ...");
     //println!("{:#?}\n\n\n\n", types);


     
     let mut builder = tonic_build::configure()
     .out_dir("src/protobuf");

     for (name, ptype, ) in types{


          builder = builder.type_attribute(name.clone(),
                "#[derive(graph_runtime_derive::ToAscObj)]");

          let asc = format!("#[asc_obj_type(Asc{})]", name);
          builder = builder.type_attribute(name.clone(),asc);

          if let Some(list) = ptype.req_fields_as_string(){
               let flds = format!("#[required({})]", list);
               builder = builder.type_attribute(name.clone(), flds);
          }


     }
     builder.compile(&["proto/type.proto"], &["proto"])
     .expect("Failed to compile Firehose Tendermint proto(s)");
     

}
