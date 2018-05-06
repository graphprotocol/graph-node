FROM rust:latest

# Install clang (required for dependencies)
RUN apt-get update \
    && apt-get install -y clang libclang-dev

# Install Python to serve the docs
RUN apt-get install python

# Copy, build, install and run thegraph-local-node
COPY . .
RUN cd thegraph-local-node && cargo doc --no-deps -p thegraph
WORKDIR target/doc/
CMD ["python", "-m", "SimpleHTTPServer", "8000"]
