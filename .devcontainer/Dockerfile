FROM mcr.microsoft.com/devcontainers/rust:0-1-bullseye

RUN rustup install 1.66.0-x86_64-unknown-linux-gnu
RUN rustup component add rustfmt-preview
RUN apt update && apt install -y protobuf-compiler