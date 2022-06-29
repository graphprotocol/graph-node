const PROT_FILE: &str = "proto/type.proto";

fn main() {
    println!("cargo:rerun-if-changed=proto");

    let types = common::parse_proto_file(PROT_FILE).expect("Unable to parse proto file!");

    let mut builder = tonic_build::configure().out_dir("src/protobuf");

    for (name, ptype) in types {
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
    }

    builder
        .compile(&["proto/type.proto"], &["proto"])
        .expect("Failed to compile Firehose Cosmos proto(s)");
}
