fn main() {
    tonic_build::configure()
        .out_dir("src/firehose")
        .format(true)
        .compile(&["proto/bstream.proto"], &["proto"])
        .expect("Failed to compile Firehose proto(s)");
}
