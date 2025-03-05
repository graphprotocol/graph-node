fn main() {
    println!("cargo:rerun-if-changed=proto");
    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .extern_path(
            ".sf.near.codec.v1",
            "::substreams_near_core::pb::sf::near::type::v1",
        )
        .out_dir("src/pb")
        .compile_protos(&["proto/receipts.proto"], &["proto"])
        .expect("Failed to compile Substreams entity proto(s)");
}
