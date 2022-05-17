fn main() {
    println!("cargo:rerun-if-changed=proto");
    tonic_build::configure()
        .out_dir("src/firehose")
        .compile(
            &[
                "proto/firehose.proto",
                "proto/ethereum/transforms.proto",
                "proto/near/transforms.proto",
                "proto/cosmos/transforms.proto",
            ],
            &["proto"],
        )
        .expect("Failed to compile Firehose proto(s)");
}
