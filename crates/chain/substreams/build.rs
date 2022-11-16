fn main() {
    println!("cargo:rerun-if-changed=proto");
    tonic_build::configure()
        .out_dir("src/protobuf")
        .compile(&["codec.proto"], &["proto"])
        .expect("Failed to compile Substreams entity proto(s)");
}
