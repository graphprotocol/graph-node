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

To use the `docker-compose.yml` configuration in this repository, simply clone
this repo and run
```sh
docker-compose up
```

This will start IPFS, Postgres, Parity and Graph Node in Docker and create persistent
data directories for IPFS and Postgres in `./data/ipfs` and `./data/postgres`. You
can access these via:

* Graph Node:
    - GraphiQL: `http://localhost:8000/`
    - HTTP: `http://localhost:8000/subgraphs/name/<subgraph-name>`
    - WebSockets: `ws://localhost:8001/subgraphs/name/<subgraph-name>`
    - JSON-RPC admin: `http://localhost:8020/`
* IPFS:
    - `127.0.0.1:5001` or `/ip4/127.0.0.1/tcp/5001`
* Parity:
    - JSON-RPC: `http://localhost:8545/`
    - WebSockets: `ws://localhost:8546/`
* Postgres:
    - `postgresql://graph-node:let-me-in@localhost:8545/graph-node`

The Parity dev chain included in this setup creates one default account and starts
with three additional unlocked account you can use for development and testing (e.g.
with Truffle).

In order to use the three unlocked accounts, you have to send money to them from
the default account. In a web3 console (e.g. `truffle console`), run
```js
> # Unlock the default account
> web3.eth.personal.unlockAccount('0x00a329c0648769A73afAc7F9381E08FB43dBEA72', '')
>
> # Send money to the other unlocked accounts
> web3.eth.sendTransaction({from: '0x00a329c0648769A73afAc7F9381E08FB43dBEA72', to:'0xddf8430d91ca7cf8df175813b58865dff2e15bc6', value: 10000000000000000000})
> web3.eth.sendTransaction({from: '0x00a329c0648769A73afAc7F9381E08FB43dBEA72', to:'0xc4ca008b1a769c4330ab6f42f53dc367d0527c60', value: 10000000000000000000})
> web3.eth.sendTransaction({from: '0x00a329c0648769A73afAc7F9381E08FB43dBEA72', to:'0xd49c572ab93dcc58627a70420763de4bdb74d6e8', value: 10000000000000000000})
```
