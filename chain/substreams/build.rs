fn main() {
    println!("cargo:rerun-if-changed=proto");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=path/to/Cargo.lock");
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .out_dir("src/protobuf")
        .compile(&["proto/codec.proto"], &["proto"])
        .expect("Failed to compile Substreams entity proto(s)");
}
