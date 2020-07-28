extern crate assert_cli;

#[test]
fn node_fails_to_start_without_postgres_url() {
    assert_cli::Assert::main_binary()
        .fails()
        .and()
        .stderr()
        .contains("error: The following required arguments were not provided:")
        .unwrap();
}

#[test]
fn node_fails_to_start_with_invalid_network_capability() {
    assert_cli::Assert::main_binary()
        .with_args(&[
            "--postgres-url",
            "postgresql://user:pass@localhost:5432/test",
            "--ethereum-rpc",
            "mainnet:everything:works:https://localhost:8545/",
            "--ipfs",
            "http://localhost:5001/",
        ])
        .fails()
        .and()
        .stderr()
        .contains("Failed to parse Ethereum networks and create Ethereum adapters: Invalid Ethereum node capability supplied: [\"everything\"]")
        .unwrap()
}

#[test]
fn node_fails_to_start_with_unnamed_ethereum_network() {
    assert_cli::Assert::main_binary()
        .with_args(&[
            "--postgres-url",
            "postgresql://user:pass@localhost:5432/test",
            "--ethereum-rpc",
            "https://localhost:8545/",
            "--ipfs",
            "http://localhost:5001/",
        ])
        .fails()
        .and()
        .stderr()
        .contains("Failed to parse Ethereum networks and create Ethereum adapters: Is your Ethereum node string missing a network name? Try 'mainnet:' + the Ethereum node URL.")
        .unwrap()
}

#[test]
fn node_fails_to_start_with_invalid_ipfs() {
    assert_cli::Assert::main_binary()
        .with_args(&[
            "--postgres-url",
            "postgresql://user:pass@localhost:5432/test",
            "--ethereum-rpc",
            "mainnet:https://localhost:8545/",
            "--ipfs",
            "http://my-node",
        ])
        .fails()
        .and()
        .stderr()
        .contains("Failed to connect to IPFS: json parse error 'expected value at line 1 column 1")
        .unwrap()
}
