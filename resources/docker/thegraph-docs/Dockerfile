FROM rust:latest

# Install clang (required for dependencies)
RUN apt-get update \
    && apt-get install -y clang libclang-dev

# Install Python to serve the docs
RUN apt-get install python

# Build all docs (dependencies and workspace crates)
COPY . .
RUN cargo doc

# Serve the docs over HTTP
WORKDIR target/doc/
CMD ["python", "-m", "SimpleHTTPServer", "8000"]
