fn main() {
    println!("cargo:rerun-if-changed=proto");
    tonic_build::configure()
        .out_dir("src/protobuf")
        .compile_protos(&["proto/arweave.proto"], &["proto"])
        .expect("Failed to compile Firehose Arweave proto(s)");
}
