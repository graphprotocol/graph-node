fn main() {
    println!("cargo:rerun-if-changed=proto");
    tonic_build::configure()
        .out_dir("src/protobuf")
        .format(true)
        .compile(&["proto/codec.proto"], &["proto"])
        .expect("Failed to compile StreamingFast NEAR proto(s)");
}
