# Display available commands and their descriptions (default target)
default:
    @just --list

# Format all Rust code (cargo fmt)
format *EXTRA_FLAGS:
    cargo fmt --all {{EXTRA_FLAGS}}

# Run Clippy linting (cargo clippy)
lint *EXTRA_FLAGS:
    cargo clippy --all-targets {{EXTRA_FLAGS}}

# Check Rust code (cargo check)
check *EXTRA_FLAGS:
    cargo check {{EXTRA_FLAGS}}

# Check all workspace members, all their targets and all their features
check-all:
    cargo check --workspace --all-features --all-targets

# Build graph-node (cargo build --bin graph-node)
build *EXTRA_FLAGS:
    cargo build --bin graph-node {{EXTRA_FLAGS}}

_cargo-test *ARGS:
    #!/usr/bin/env bash
    set -e # Exit on error

    # Ensure that the `THEGRAPH_STORE_POSTGRES_DIESEL_URL` environment variable is set.
    if [ -z "$THEGRAPH_STORE_POSTGRES_DIESEL_URL" -a -z "$GRAPH_NODE_TEST_CONFIG" ]; then
        echo "error: Neither THEGRAPH_STORE_POSTGRES_DIESEL_URL nor GRAPH_NODE_TEST_CONFIG is set"
        exit 1
    fi

    cargo test {{ARGS}}

# Run all tests (unit and integration)
test *EXTRA_FLAGS:
    @just _cargo-test {{EXTRA_FLAGS}} --workspace -- --nocapture

# Run unit tests
test-unit *EXTRA_FLAGS:
    @just _cargo-test {{EXTRA_FLAGS}} --workspace --exclude graph-tests -- --nocapture

# Run runner tests
test-runner *EXTRA_FLAGS:
    @just _cargo-test {{EXTRA_FLAGS}} --package graph-tests --test runner_tests -- --nocapture

# Run integration tests
test-integration *EXTRA_FLAGS:
    @just _cargo-test {{EXTRA_FLAGS}} --package graph-tests --test integration_tests -- --nocapture

# Clean workspace (cargo clean)
clean:
    cargo clean

compile-contracts:
    #!/usr/bin/env bash
    set -e # Exit on error

    if ! command -v "forge" &> /dev/null; then
        echo "Error: forge must be on your path"
        exit 1
    fi

    cd tests/contracts

    forge build

    mkdir -p abis
    for c in src/*.sol
    do
        contract=$(basename $c .sol)
        echo $contract
        forge inspect --json "$contract" abi > "abis/$contract.json"
    done
