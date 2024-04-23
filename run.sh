#!/usr/bin/env bash
export GRAPH_LOG=info
export GRAPH_ALLOW_NON_DETERMINISTIC_IPFS=true
export GRAPH_ALLOW_NON_DETERMINISTIC_FULLTEXT_SEARCH=true

export NODE="34.141.208.199"
# export NODE="127.0.0.1"
# export NODE="34.147.84.221"
export NODE=34.32.175.76

eval $(sed < .env -n -e 's/^\(.*\)$/export \1/p')

exec cargo run \
  --bin=graph-node \
  --package=graph-node \
  --profile=dev \
  -- \
  --node-id default \
  --postgres-url "postgresql://graph-node:let-me-in@127.0.0.1:5432/graph-node" \
  --ethereum-rpc "mainnet:http://$NODE:8545" \
  --ipfs "localhost:5001" 2>&1
