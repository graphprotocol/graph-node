fn main() {
    tonic_build::configure()
        .out_dir("src/protobuf")
        .format(cfg!(debug_assertions)) // Release build environments might not have rustfmt installed
        .compile(&["proto/codec.proto"], &["proto"])
        .expect("Failed to compile StreamingFast Ethereum proto(s)");
}
