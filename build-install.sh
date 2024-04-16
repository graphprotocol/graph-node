#!/usr/bin/env bash
mkdir -p /usr/local/bin
cargo build --release
cp target/release/graph-node /usr/local/bin/
