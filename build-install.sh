#!/usr/bin/env bash
mkdir -p /usr/local/bin
cargo build --release --bins -j 1
cp target/release/graph-node target/release/graphman /usr/local/bin/
