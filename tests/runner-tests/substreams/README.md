# Substreams-powered subgraph: tracking contract creation

A basic Substreams-powered subgraph, including the Substreams definition. This example detects new
contract deployments on Ethereum, tracking the creation block and timestamp. There is a
demonstration of the Graph Node integration, using `substreams_entity_change` types and helpers.

## Prerequisites

This
[requires the dependencies necessary for local Substreams development](https://substreams.streamingfast.io/developers-guide/installation-requirements).

## Quickstart

```
pnpm install # install graph-cli
pnpm substreams:prepare # build and package the substreams module
pnpm subgraph:build # build the subgraph
pnpm subgraph:deploy # deploy the subgraph
```
