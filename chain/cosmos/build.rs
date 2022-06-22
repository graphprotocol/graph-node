const PROT_FILE: &str = "proto/type.proto";

fn main() {
    println!("cargo:rerun-if-changed=proto");

    let types = common::parse_proto_file(PROT_FILE).expect("Unable ...");

    let types_to_skip = vec![
        //"PublicKey", //cannot introspect enum type
    ];

    let mut builder = tonic_build::configure().out_dir("src/protobuf");

    for (name, ptype) in types {
        if types_to_skip.contains(&name.as_str()) {
            continue;
        }

        //generate Asc<Type>
        builder = builder.type_attribute(
            name.clone(),
            format!(
                "#[graph_runtime_derive::generate_asc_type({})]",
                ptype.fields().unwrap_or_default()
            ),
        );

        //generate data index id
        builder = builder.type_attribute(
            name.clone(),
            "#[graph_runtime_derive::generate_network_type_id(Cosmos)]",
        );

        //generate conversion from rust type to asc
        builder = builder.type_attribute(
            name.clone(),
            format!(
                "#[graph_runtime_derive::generate_from_rust_type({})]",
                ptype.fields().unwrap_or_default()
            ),
        );

        // //fix padding
        // builder = builder.type_attribute(name.clone(),
        //     "#[graph_runtime_derive::generate_padding]");
    }

    builder
        .compile(&["proto/type.proto"], &["proto"])
        .expect("Failed to compile Firehose Cosmos proto(s)");
}

// #[graph_runtime_derive::generate_asc_type] //Asc<Type>
// #[graph_runtime_derive::generate_network_type_id(Cosmos)] //<>
// #[graph_runtime_derive::generate_from_rust_type(field_01, field_02)]
