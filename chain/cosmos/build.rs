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


    let types_to_skip = vec!["PublicKey"];

    //this this temporary, for development
    let types_to_include = vec!["Consensus", "Timestamp"]; //, "Block", "Header", "EvidenceList"];

    let mut builder = tonic_build::configure().out_dir("src/protobuf");


    for (name, ptype) in types {
        // if name == "Block"{ continue;  }

        if types_to_skip.contains(&name.as_str()){
            continue;
        }

        if !types_to_include.contains(&name.as_str()){
            continue;
        }

        //
        builder = builder.type_attribute(name.clone(), "#[derive(graph_runtime_derive::GenerateAscType)]");
        builder = builder.type_attribute(name.clone(), "#[chain_name(Cosmos)]");

        builder = builder.type_attribute(name.clone(), "#[derive(graph_runtime_derive::ToAscObj)]");
        let asc = format!("#[asc_obj_type(Asc{})]", name);
        builder = builder.type_attribute(name.clone(), asc);

        if let Some(list) = ptype.req_fields_as_string() {
            let flds = format!("#[required({})]", list);
            builder = builder.type_attribute(name.clone(), flds);
        }




    }
    builder
        .compile(&["proto/type.proto"], &["proto"])
        .expect("Failed to compile Firehose Cosmos proto(s)");
}
