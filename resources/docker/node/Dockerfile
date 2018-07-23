FROM rust:latest

# Install clang (required for dependencies)
RUN apt-get update \
    && apt-get install -y clang libclang-dev

# Copy, build, install and run thegraph-node
COPY . .
RUN cd node && cargo install --bins
CMD ["thegraph-node"]
