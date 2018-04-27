FROM rust:latest
COPY . .
RUN cd thegraph-standalone-node && cargo install --bins
CMD ["thegraph-standalone-node"]
