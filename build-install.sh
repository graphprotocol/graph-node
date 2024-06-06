#!/usr/bin/env bash
mkdir -p /usr/local/bin
cargo build --release --bins -j 1
HAS_SERVICE=
if which systemctl
then
    HAS_SERVICE=1
fi
if [ -n "$HAS_SERVICE" ]
then
    sudo systemctl stop graph
fi
sudo cp target/release/graph-node target/release/graphman /usr/local/bin/
if [ -n "$HAS_SERVICE" ]
then
    sudo systemctl start graph
fi
