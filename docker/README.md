# Graph Node Docker Image

Preconfigured Docker image for running a Graph Node.

## Usage

```sh
docker run -it \
  -e postgres_host=<HOST>[:<PORT>] \
  -e postgres_user=<USER> \
  -e postgres_pass=<PASSWORD> \
  -e postgres_db=<DBNAME> \
  -e ipfs=<HOST>:<PORT> \
  -e ethereum=<NETWORK_NAME>:<ETHEREUM_RPC_URL>
```

### Example usage

```sh
docker run -it \
  -e postgres_host=host.docker.internal:5432
  -e postgres_user=graph-node \
  -e postgres_pass=oh-hello \
  -e postgres_db=graph-node \
  -e ipfs=host.docker.internal:5001 \
  -e ethereum=mainnet:https://mainnet.infura.io
```

## Docker Compose

The Docker Compose setup requires an Ethereum network name and node
to connect to. By default, it will use `dev:http://docker.host.internal:8545`
in order to connect to an Ethereum node running on your host machine.
You can replace this with anything else in `docker-compose.yaml`.

After you have set up an Ethereum node—e.g. Ganache or Parity—, simply
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
  - `postgresql://graph-node:let-me-in@localhost:8545/graph-node`

Once this is up and running, you can use
[`graph-cli`](https://github.com/graphprotocol/graph-cli) to create and
deploy your subgraph to the running Graph Node.
