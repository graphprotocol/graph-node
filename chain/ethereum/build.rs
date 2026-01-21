fn main() {
    println!("cargo:rerun-if-changed=proto");

    tonic_build::configure()
        .out_dir("src/protobuf")
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&["proto/ethereum.proto"], &["proto"])
        .expect("Failed to compile Firehose Ethereum proto(s)");
}
