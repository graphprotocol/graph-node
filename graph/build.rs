fn main() {
    tonic_build::configure()
        .compile(&["proto/bstream.proto"], &["proto"])
        .unwrap_or_else(|e| panic!("Failed to compile StreamingFast proto(s) {:?}", e));
}
