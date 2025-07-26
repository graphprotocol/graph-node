# Running prebuilt `graph-node` images

You can run the `graph-node` docker image either in a [complete
setup](#docker-compose) controlled by Docker Compose, or, if you already
have an IPFS and Postgres server, [by
itself](#running-with-existing-ipfs-and-postgres).

## Docker Compose

The Docker Compose setup requires an Ethereum network name and node
to connect to. By default, it will use `mainnet:http://host.docker.internal:8545`
in order to connect to an Ethereum node running on your host machine.
You can replace this with anything else in `docker-compose.yaml`.

After you have set up an Ethereum node—e.g. Ganache or Parity—simply
clone this repository and run

```sh
docker-compose up
```

This will start IPFS, Postgres and Graph Node in Docker and create persistent
data directories for IPFS and Postgres in `./data/ipfs` and `./data/postgres`. You
can access these via:

- Graph Node:
  - GraphiQL: `http://localhost:8000/`
  - HTTP: `http://localhost:8000/subgraphs/name/<subgraph-name>`
  - WebSockets: `ws://localhost:8001/subgraphs/name/<subgraph-name>`
  - Admin: `http://localhost:8020/`
- IPFS:
  - `127.0.0.1:5001` or `/ip4/127.0.0.1/tcp/5001`
- Postgres:
  - `postgresql://graph-node:let-me-in@localhost:5432/graph-node`

Once this is up and running, you can use
[`graph-cli`](https://github.com/graphprotocol/graph-tooling/tree/main/packages/cli) to create and
deploy your subgraph to the running Graph Node.

### Running Graph Node on an Macbook M1

We do not currently build native images for Macbook M1, which can lead to processes being killed due to out-of-memory errors (code 137). Based on the example `docker-compose.yml` is possible to rebuild the image for your M1 by running the following, then running `docker-compose up` as normal:

> **Important** Increase memory limits for the docker engine running on your machine. Otherwise docker build command will fail due to out of memory error. To do that, open docker-desktop and go to Resources/advanced/memory.

```
# Remove the original image
docker rmi graphprotocol/graph-node:latest

# Build the image
./docker/build.sh

# Tag the newly created image
docker tag graph-node graphprotocol/graph-node:latest
```

## Running with existing IPFS and Postgres

```sh
docker run -it \
  -e postgres_host=<HOST> \
  -e postgres_port=<PORT> \
  -e postgres_user=<USER> \
  -e postgres_pass=<PASSWORD> \
  -e postgres_db=<DBNAME> \
  -e ipfs=<HOST>:<PORT> \
  -e ethereum=<NETWORK_NAME>:<ETHEREUM_RPC_URL> \
  graphprotocol/graph-node:latest
```

## Running with emulator

There are certain chains that do not support the `eth_getBlockReceipts` method which is required for indexing (e.g. Oasis Sapphire). We can run a graph node with an emulator to support this method.

Cd into the `emulator` directory and install dependencies:

```sh
npm install
```

In docker-compose.yml, do the following:

- Uncomment the entire `emulator` service block.
- Replace `<YOUR_CHAIN_RPC_URL>` with your actual RPC URL (e.g. from Chainstack).
- Uncomment the `depends_on` line for emulator.
- Comment out the default Ethereum RPC under `graph-node` and instead uncomment the emulator RPC line.

Example:

```yaml
# emulator:
#   build: ./emulator
#   ports:
#     - '8545:8545'
#   environment:
#     UPSTREAM_RPC: https://oasis-sapphire-mainnet.core.chainstack.com/<your-key>

# ...
# ethereum: 'oasis:http://emulator:8545'  # Use this
```

Once done, save and run:

```sh
docker-compose up
```
