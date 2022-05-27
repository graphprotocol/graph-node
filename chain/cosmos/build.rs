const PROT_FILE: &str = "proto/type.proto";

fn main() {
    println!("cargo:rerun-if-changed=proto");

    let types = common::parse_proto_file(PROT_FILE).expect("Unable ...");


    let types_to_skip = vec![
        "PublicKey", //complex decoding
        "ModeInfo", //oneof, enumeration
        "Evidence", //oneof, enumeration
        "ModeInfoSingle", //enumeration
        "ModeInfoMulti",
        "DuplicateVoteEvidence",
        "LightClientAttackEvidence"
    ];

    let mut builder = tonic_build::configure().out_dir("src/protobuf");


    for (name, ptype) in types {

        if types_to_skip.contains(&name.as_str()){
            continue;
        }

        //generate Asc<Type>
        //builder = builder.type_attribute(name.clone(), "#[graph_runtime_derive::generate_asc_type]");
        builder = builder.type_attribute(name.clone(), "#[derive(graph_runtime_derive::ToAscType)]");

        // //generate data index id
        // builder = builder.type_attribute(name.clone(), "#[graph_runtime_derive::generate_network_type_id(Cosmos)]");


        let flds = format!("#[graph_runtime_derive::generate_from_rust_type({})]", ptype.req_fields_as_string().unwrap_or_default());
        builder = builder.type_attribute(name.clone(), flds);


        //     //handle struct with enumeration
        //     if let Some(list) = ptype.enum_fields_as_string() {
        //         let flds = format!("#[enum_data({})]", list);
        //         builder = builder.type_attribute(name.clone(), flds);
        //     }


    }

    builder
        .compile(&["proto/type.proto"], &["proto"])
        .expect("Failed to compile Firehose Cosmos proto(s)");
}


// #[graph_runtime_derive::generate_asc_type] //Asc<Type>
// #[graph_runtime_derive::generate_network_type_id(Cosmos)] //<>
// #[graph_runtime_derive::generate_from_rust_type(field_01, field_02)]
