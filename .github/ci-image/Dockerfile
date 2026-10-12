# An image graph-node's CI jobs can run in: rust's slim image plus the
# packages the builds need: git for actions/checkout, the lld linker, make
# and perl for the vendored OpenSSL, pkg-config and libpq for diesel when gnd's
# bundled libpq isn't in the build, and protoc with the well-known protos for
# the firehose and ethereum protos. Built and published per tag by
# ci-image.yml.
ARG RUST_VERSION
FROM rust:${RUST_VERSION}-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        git lld make perl pkg-config libpq-dev protobuf-compiler libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*
