use anyhow::{Ok, Result};
use substreams_ethereum::Abigen;

fn main() -> Result<(), anyhow::Error> {
    Abigen::new("pool", "abis/pool.json")?
        .generate()?
        .write_to_file("src/abi/pool.rs")?;
    Abigen::new("erc20", "abis/ERC20.json")?
        .generate()?
        .write_to_file("src/abi/erc20.rs")?;
    Abigen::new("factory", "abis/factory.json")?
        .generate()?
        .write_to_file("src/abi/factory.rs")?;
    Abigen::new("positionmanager", "abis/NonfungiblePositionManager.json")?
        .generate()?
        .write_to_file("src/abi/positionmanager.rs")?;

    tonic_build::configure()
        .protoc_arg("--experimental_allow_proto3_optional")
        .include_file("mod.rs")
        .out_dir("src/proto")
        .compile(&["proto/uniswap.proto"], &["proto"])
        .expect("Failed to compile Uniswap proto(s)");

    Ok(())
}
