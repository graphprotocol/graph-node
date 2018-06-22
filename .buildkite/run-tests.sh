#!/bin/bash

# Exit on the first error
set -e

# Print every command we're running
set -x

function set_up {
    # Make Postgres commands available
    export PATH="$PATH:/usr/lib/postgresql/9.6/bin"

    # Create a "test" database
    createdb test

    # Export a URL to the test database
    export THEGRAPH_STORE_POSTGRES_DIESEL_URL="postgresql://buildkite:buildkite@127.0.0.1:5432/test"
}

function tear_down {
    # Delete the test database
    dropdb test
}

# Set everything up
set_up

# Make sure to clean up before exiting
trap tear_down EXIT

# Run the test suite
cargo test -- --test-threads=1
