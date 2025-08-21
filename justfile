# Display available commands and their descriptions (default target)
default:
    @just --list

# Format all Rust code (cargo fmt)
format *EXTRA_FLAGS:
    cargo fmt --all {{EXTRA_FLAGS}}

# Check Rust code (cargo check)
check *EXTRA_FLAGS:
    cargo check {{EXTRA_FLAGS}}

# Build graph-node (cargo build --bin graph-node)
build *EXTRA_FLAGS:
    cargo build --bin graph-node {{EXTRA_FLAGS}}

# Run all tests (unit and integration)
test *EXTRA_FLAGS:
    #!/usr/bin/env bash
    set -e # Exit on error

    # Ensure that the `THEGRAPH_STORE_POSTGRES_DIESEL_URL` environment variable is set.
    if [ -z "$THEGRAPH_STORE_POSTGRES_DIESEL_URL" ]; then
        echo "Error: THEGRAPH_STORE_POSTGRES_DIESEL_URL is not set"
        exit 1
    fi

    if command -v "cargo-nextest" &> /dev/null; then
        cargo nextest run {{EXTRA_FLAGS}} --workspace
    else
        cargo test {{EXTRA_FLAGS}} --workspace -- --nocapture
    fi

# Run unit tests
test-unit *EXTRA_FLAGS:
    #!/usr/bin/env bash
    set -e # Exit on error

    # Ensure that the `THEGRAPH_STORE_POSTGRES_DIESEL_URL` environment variable is set.
    if [ -z "$THEGRAPH_STORE_POSTGRES_DIESEL_URL" ]; then
        echo "Error: THEGRAPH_STORE_POSTGRES_DIESEL_URL is not set"
        exit 1
    fi

    if command -v "cargo-nextest" &> /dev/null; then
        cargo nextest run {{EXTRA_FLAGS}} --workspace --exclude graph-tests
    else
        cargo test {{EXTRA_FLAGS}} --workspace --exclude graph-tests -- --nocapture
    fi

# Run runner tests
test-runner *EXTRA_FLAGS:
    #!/usr/bin/env bash
    set -e # Exit on error

    # Ensure that the `THEGRAPH_STORE_POSTGRES_DIESEL_URL` environment variable is set.
    if [ -z "$THEGRAPH_STORE_POSTGRES_DIESEL_URL" ]; then
        echo "Error: THEGRAPH_STORE_POSTGRES_DIESEL_URL is not set"
        exit 1
    fi

    if command -v "cargo-nextest" &> /dev/null; then
        cargo nextest run {{EXTRA_FLAGS}} --package graph-tests --test runner_tests
    else
        cargo test {{EXTRA_FLAGS}} --package graph-tests --test runner_tests -- --nocapture
    fi

# Run integration tests
test-integration *EXTRA_FLAGS:
    #!/usr/bin/env bash
    set -e # Exit on error

    if command -v "cargo-nextest" &> /dev/null; then
        cargo nextest run {{EXTRA_FLAGS}} --package graph-tests --test integration_tests
    else
        cargo test {{EXTRA_FLAGS}} --package graph-tests --test integration_tests -- --nocapture
    fi

# Clean workspace (cargo clean)
clean:
    cargo clean
