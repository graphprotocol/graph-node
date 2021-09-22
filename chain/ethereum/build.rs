fn main() {
    tonic_build::configure()
        .out_dir("src/protobuf")
        .format(true)
        .compile(&["proto/codec.proto"], &["proto"])
        .expect("Failed to compile StreamingFast Ethereum proto(s)");
}
