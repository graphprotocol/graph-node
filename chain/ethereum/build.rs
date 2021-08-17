fn main() {
    tonic_build::configure()
        .compile(&["proto/codec.proto"], &["proto"])
        .unwrap_or_else(|e| panic!("Failed to compile StreamingFast Ethereum proto(s) {:?}", e));
}
