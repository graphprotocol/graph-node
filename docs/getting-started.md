# Getting Started
> **Note**:  this project is heavily WIP and until it reaches v1.0 the API is subject to change in breaking ways without notice.

## 1 Introduction
The Graph is a decentralized network of nodes, which indexes and processes queries against data from blockchains. Its purpose is to be an essential layer in the [Web3 stack](https://multicoin.capital/2018/07/10/the-web3-stack/), helping developers build truly unstoppable, censor-resistant decentralized applications on a platform which supports rich interoperability - in direct contrast to today's data monopolies.

Graph Node is the first step towards that vision, as it implements much of the core indexing and querying capabilities that will be available in the final decentralized network. It can be run locally (or hosted on centralized infrastructure) and provides a bespoke GraphQL interface for your dApp, serving data from the Ethereum blockchain required for your specific use case.

This is already significant in that it cuts down on significant duplicated effort currently being incurred across dApp developer community. Developers can focus on what makes their dApp unique--their schema and data transformations--and Graph Node takes care of the cross-cutting concerns that are shared between all dApps - spinning up a database, connecting to the Ethereum blockchain, handling edge cases such as block reorgs and providing a tasteful and consistent API for querying collections of entities, traversing entity relationships, etc.

## 2 Overview
In order to do anything useful with The Graph, you'll need to define a *subgraph* which specifies the GraphQL schema for your dApp, the source data on blockchain your dApp will use (i.e. an Ethereum smart contract) and a mapping which transforms and loads your data into a store with your specific schema.

Once you've defined your subgraph you can then deploy it to your locally running Graph Node (and in the future to The Graph network).

## 3 Defining your Subgraph
The subgraph is defined as a YAML file called a *subgraph manifest*. See [here](https://github.com/graphprotocol/graph-cli/blob/master/examples/example-event-handler/data-source.yaml) for an example, or [here](graphql-api.md) for the full subgraph manifest specification.

The logical first places to start defining your subgraph are the GraphQL schema and which smart contracts will be indexed. This will enable you to start writing mappings against the schema and smart contracts. Our toolchain is javascript-based so you'll want to define these in a new repo or a directory with it's own `package.json`.

### 3.1 Defining your GraphQL schema
GraphQL schemas are defined using the GraphQL interface definition language (IDL). If you've never written a GraphQL schema, we recommend checking out a [quick primer](https://graphql.org/learn/schema/#type-language) on the GraphQL type system.

With The Graph, you don't have to define your own top-level `Query` type, you simply define entity types, and Graph Node will generate top level fields for querying single instances and collections of that entity type.

##### Example
Define a simple Token entity type:
```graphql
type Token {
  id: ID!
  name: String!
  minted: Int!
}
```

Later, when you've finally deployed your subgraph with this entity, you'll be able to query for Tokens:

```graphql
query {
  token(id: "123") {
    name
    minted
  }
}
```

or

```graphql
query {
  tokens(orderBy: minted) {
    id
    name
    minted
  }
}
```

See the [Schema API](graphql-api.md#3-schema) for a complete reference on defining your schema for The Graph.

Once you've completed your schema, add the path of the schema to the top level `schema` key in your subgraph manifest.

### 3.2 Defining your source data
Each data source in your subgraph is comprised of data on blockchain (i.e. an Ethereum smart contract) and a mapping which transforms and loads that data onto The Graph.

These are defined the top-level `dataSources` key in the subgraph manifest.

##### Example
Defining a data source which is a smart contract implementing the ERC20 interface:
```yaml
dataSources:
- kind: ethereum/contract
  name: MyERC20Contract
  source:
    address: "f87e31492faf9a91b02ee0deaad50d51d56d5d4d"
    abi: ERC20
  mapping:
    kind: ethereum/events
    apiVersion: 0.0.1
    language: wasm/assemblyscript
    entities:
    - Parcel
    - ParcelData
    abis:
    - name: ERC20
      file: ./abis/ERC20ABI.json
    eventHandlers:
    - event: Transfer(address,address,uint)
      handler: handleTransfer
    file: ./mapping.ts
```

### 3.3 Generate types for your mapping with the Graph-CLI
Using `yarn` or `npm` (the examples in this doc use `yarn`) install The Graph CLI directly from the Github repo into your subgraph directory.

Follow the instructions in the repo's README for setting up your tsconfig and package.json scripts.

Then, in your shell run:
```shell
yarn run codegen
```

What this command does is it looks at the contract ABIs defined in your `dataSources` manifests, and for the respective mapping it generates TypeScript types (actually AssemblyScript types, but more on that later) for the smart contracts your mappings script will interface with, including the types of public methods and events.

This is incredibly useful for writing correct mappings, as well as improving developer productivity using the TypeScript language support in your favorite editor or IDE.

### 3.4 Write your mappings
Mappings are written in a subset of TypeScript called AssemblyScript which can be compiled down to WASM. AssemblyScript is stricter than normal TypeScript, yet provides a familiar syntax. A few TypeScript/Javascript features which are not supported in AssemblyScript include plain old Javascript objects (POJOs), untyped arrays, untyped maps, union types, the `any` type and variadic functions. `switch` statements also work differently. See [the AssemblyScript wiki](https://github.com/AssemblyScript/assemblyscript/wiki) for a full reference on AssemblyScript.

In your mapping file, create named export functions corresponding to the names specified in your subgraph manifest.

Each handler should accept a single parameter called `event` with a type corresponding to the name of the event which is being handled (this type was generated for you in the previous step).

##### Example
```typescript
export function handleTransfer(event: Transfer): void {
  // Event handler logic goes here
}
```

As mentioned, AssemblyScript does not have untyped maps or plain old Javascript objects, so to represent a collection of key value tuples with heterogeneous types, a global `Entity` type is included in the mapping types.

The `Entity` type has different setter methods for different types, satisfying AssemblyScript's requirement of strictly typed functions (and no union or `any` types).

#### Example
```typescript
let parcel = new Entity()
  token.setString('name', "MyToken")
  token.setAddress('owner', event.params.to)
  token.setU256('amount', event.params.tokens)
```

There is also a global `Store` class which has a `set` method for setting the value(s) of a particular entity's attribute(s) in the store.

It expects the name of an entity type, the id of the entity and the `Entity` itself.

Before `set` may be called a `Store` instance must be created by calling `bind` to bind the `Store` instance to a specific block hash. All store transactions created with this `Store` instance will be associated with that block hash.

##### Example
```typescript
  let store = Store.bind(event.blockHash)
  store.set('Token', tokenId, token)
```

The eventHandlers functions return `void`. The only way that entities may be added to the The Graph is by calling `Store.set()`. `Store.set()` may be called multiple times in an event handler.

## 4 Build
## 4.1 Compile your mappings
To compile your mappings run `yarn build` in your subgraph directory.

This is useful for verifying that your mappings and subgraph manifest were written correctly.

## 4.2 Deploy your mappings to IPFS
In order to deploy your subgraph to your Graph Node, the subgraph manifest will first need to be deployed to IPFS (along with all linked files).

Follow the instructions [here](https://ipfs.io/docs/getting-started/) to start a locally running IPFS daemon.

Once you've started your IPFS daemon you can run `yarn build-ipfs` in your subgraph directory (it assumes the default tcp port for the IPFS daemon).

This will compile your mappings, and deploy the mappings, schema and the subgraph manifest itself to IPFS. The result should be a single content hash.

You can pass that content hash into `ipfs cat` to view your subgraph manifest with files paths replaced by IPLD links.

## 4.3 Deploy your subgraph to your local Graph Node
Follow the instructions in the [Graph Node README](https://github.com/graphprotocol/graph-node) for deploying your subgraph to a locally running Graph Node using your subgraph's IPFS content hash.

## 5 Query your local Graph Node
With your subgraph deployed to your locally running Graph Node, visit http://127.0.0.1:8000/ to open up a [Graphiql](https://github.com/graphql/graphiql) interface where you can explore your deployed GraphQL API for your subgraph by issuing queries and viewing the schema.

See the [Query API](graphql-api.md#1-queries) for a complete reference on how to query your subgraph's entities.

#### Example
Query all `Token` entities:
```graphql
query {
  tokens {
    id
    owner
  }
}
```

## 6 Buidl 🚀
Start building world-changing dApps on top of your newly deployed GraphQL interface 🗿✨.

Feedback and contributions in the form of issues and pull requests are welcome!
