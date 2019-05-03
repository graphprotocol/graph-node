# Subgraph Manifest
##### v.0.0.1

## 1.1 Overview
The subgraph manifest specifies all the information required to index and query a specific subgraph. This is the entry point to your subgraph.

The subgraph manifest, and all the files linked from it, is what is deployed to IPFS and hashed to produce a subgraph ID that can be referenced and used to retrieve your subgraph in The Graph.

## 1.2 Format
Any data format that has a well-defined 1:1 mapping with the [IPLD Canonical Format](https://github.com/ipld/specs/) may be used to define a subgraph manifest. This includes YAML and JSON. Examples in this document are in YAML.

## 1.3 Top-Level API

| Field  | Type | Description   |
| --- | --- | --- |
| **specVersion** | *String*   | A Semver version indicating which version of this API is being used.|
| **schema**   | [*Schema*](#14-schema) | The GraphQL schema of this subgraph.|
| **description**   | *String* | An optional description of the subgraph's purpose. |
| **repository**   | *String* | An optional link to where the subgraph lives. |
| **dataSources**| [*Data Source Spec*](#15-data-source)| Each data source spec defines the data that will be ingested as well as the transformation logic to derive the state of the subgraph's entities based on the source data.|

## 1.4 Schema

| Field | Type | Description |
| --- | --- | --- |
| **file**| [*Path*](#16-path) | The path of the GraphQL IDL file, either local or on IPFS. |

## 1.5 Data Source

| Field | Type | Description |
| --- | --- | --- |
| **kind** | *String | The type of data source. Possible values: *ethereum/contract*.|
| **name** | *String* | The name of the source data. Will be used to generate APIs in the mapping and also for self-documentation purposes. |
| **network** | *String* | For blockchains, this describes which network the subgraph targets. For Ethereum, this could be, for example, "mainnet" or "rinkeby". |
| **source** | [*EthereumContractSource*](#151-ethereumcontractsource) | The source data on a blockchain such as Ethereum. |
| **mapping** | [*Mapping*](#152-mapping) | The transformation logic applied to the data prior to being indexed. |
| **templates** | [*Dynamic Data Source Spec*](#17-dynamic-data-source) | Each dynamic data source spec defines a template of a traditional data source. This templates will be created and mapped to a live data source at run-time. |

### 1.5.1 EthereumContractSource

| Field | Type | Description |
| --- | --- | --- |
| **address** | *String* | The address of the source data in its respective blockchain. |
| **abi** | *String* | The name of the ABI for this Ethereum contract. See `abis` in the `mapping` manifest. |

### 1.5.2 Mapping
The `mapping` field may be one of the following supported mapping manifests:
 - [Ethereum Events Mapping](#1521-ethereum-events-mapping)

#### 1.5.2.1 Ethereum Events Mapping

| Field | Type | Description |
| --- | --- | --- |
| **kind** | *String* | Must be "ethereum/events" for Ethereum Events Mapping. |
| **apiVersion** | *String* | Semver string of the version of the Mappings API that will be used by the mapping script. |
| **language** | *String* | The language of the runtime for the Mapping API. Possible values: *wasm/assemblyscript*. |
| **entities** | *[String]* | A list of entities that will be ingested as part of this mapping. Must correspond to names of entities in the GraphQL IDL. |
| **abis** | *ABI* | ABIs for the contract classes that should be generated in the Mapping ABI. Name is also used to reference the ABI elsewhere in the manifest. |
| **eventHandlers** | *EventHandler* | Handlers for specific events, which will be defined in the mapping script. |
| **file** | [*Path*](#16-path) | The path of the mapping script. |

#### 1.5.2.2 EventHandler

| Field | Type | Description |
| --- | --- | --- |
| **event** | *String* | An identifier for an event that will be handled in the mapping script. For Ethereum contracts, this must be the full event signature to distinguish from events that may share the same name. No alias types can be used. For example, uint will not work, uint256 must be used.|
| **handler** | *String* | The name of an exported function in the mapping script that should handle the specified event. |
| **topic0** | optional *String* | A `0x` prefixed hex string. If provided, events whose topic0 is equal to this value will be processed by the given handler. When topic0 is provided, _only_ the topic0 value will be matched, and not the hash of the event signature. This is useful for processing anonymous events in Solidity, which can have their topic0 set to anything.  By default, topic0 is equal to the hash of the event signature. |

## 1.6 Path
A path has one field `path`, which either refers to a path of a file on the local dev machine or an [IPLD link](https://github.com/ipld/specs/).

When using the Graph-CLI, local paths may be used during development, and then, the tool will take care of deploying linked files to IPFS and replacing the local paths with IPLD links at deploy time.

| Field | Type | Description |
| --- | --- | --- |
| **path** | *String or [IPLD Link](https://github.com/ipld/specs/)* | A path to a local file or IPLD link. |

## 1.7 Dynamic Data Source
A dynamic data source has all of the fields of a normal data source, except the [EthereumContractSource](#151-ethereumcontractsource) address field is removed.
```yml
# ...
dataSources:
  - kind: ethereum/contract
    name: Factory
    # ...
    templates:
      - name: Exchange
        kind: ethereum/contract
        network: mainnet
        source:
          abi: Exchange
        mapping:
          kind: ethereum/events
          apiVersion: 0.0.1
          language: wasm/assemblyscript
          file: ./src/mappings/exchange.ts
          entities:
            - Exchange
          abis:
            - name: Exchange
              file: ./abis/exchange.json
          eventHandlers:
            - event: TokenPurchase(address,uint256,uint256)
              handler: handleTokenPurchase
```
