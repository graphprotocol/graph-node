# Full build with debuginfo for graph-node
#
# The expectation if that the docker build uses the parent directory as PWD
# by running something like the following
#   docker build --target STAGE -f docker/Dockerfile .

FROM rust:alpine as graph-node-bld

ARG COMMIT_SHA=unknown
ARG REPO_NAME=unknown
ARG BRANCH_NAME=unknown
ARG TAG_NAME=unknown

ADD . /graph-node

# See https://github.com/rust-lang/cargo/issues/7563 for why we use -crt-static
# in the RUSTFLAGS
RUN apk update \
 && apk add musl-dev openssl-dev postgresql-dev git \
 && cd /graph-node \
 && RUSTFLAGS="-g -C target-feature=-crt-static" cargo install --locked --path node \
 && cargo clean \
 && objcopy --only-keep-debug /usr/local/cargo/bin/graph-node /usr/local/cargo/bin/graph-node.debug \
 && strip -g /usr/local/cargo/bin/graph-node \
 && cd /usr/local/cargo/bin \
 && objcopy --add-gnu-debuglink=graph-node.debug graph-node \
 && echo "repo:   $REPO_NAME" > /image-info \
 && echo "tag:    $TAG_NAME" >> /image-info \
 && echo "branch: $BRANCH_NAME" >> /image-info \
 && echo "commit: $COMMIT_SHA" >> /image-info \
 && echo "cargo:  $(cargo --version)" >> /image-info \
 && echo "rust:   $(rustc --version)" >> /image-info

# The graph-node runtime image with only the executable
FROM alpine:latest as graph-node-run
ENV RUST_LOG ""
ENV GRAPH_LOG ""
ENV EARLY_LOG_CHUNK_SIZE ""
ENV ETHEREUM_RPC_PARALLEL_REQUESTS ""
ENV ETHEREUM_BLOCK_CHUNK_SIZE ""

ENV postgres_host ""
ENV postgres_user ""
ENV postgres_pass ""
ENV postgres_db ""
ENV ipfs ""
# The etherum network(s) to connect to. Set this to a space-separated
# list of the networks where each entry has the form NAME:URL
ENV ethereum ""

# HTTP port
EXPOSE 8000
# WebSocket port
EXPOSE 8001
# JSON-RPC port
EXPOSE 8020

RUN apk add libgcc libssl1.1 libcrypto1.1 postgresql-libs bash

ADD docker/wait_for docker/start /usr/local/bin/
COPY --from=graph-node-bld /usr/local/cargo/bin/graph-node /usr/local/bin
COPY --from=graph-node-bld /image-info /image-info
COPY docker/Dockerfile /Dockerfile

# The query-node image
FROM graph-node-run as query-node
CMD start query-node

# The index-node image
FROM graph-node-run as index-node
CMD start index-node

# The combined-node image used for the public docker compose setup
FROM graph-node-run as graph-node
CMD start combined-node

# Debug image to access core dumps
FROM graph-node-bld as graph-node-dbg
RUN apk add gdb
ADD docker/debug /usr/local/bin
COPY --from=graph-node-bld /image-info /image-info
COPY docker/Dockerfile /Dockerfile
