# The Graph CLI

This package installs the CLI for developing subgraphs for [The Graph
Network](https://thegraph.com/subgraphs/) It supports all aspects of
[developing, testing, and deploying subgraphs](https://thegraph.com/docs/en/subgraphs/quick-start/) locally and
on the network.

This package is a complete replacement for the older
[graph-cli](https://www.npmjs.com/package/@graphprotocol/graph-cli) which
will over time be migrated to be a simple wrapper for `gnd`. Older
documentation will reference `graph-cli` in its instructions; running
`alias graph=gnd` in the shell make it possible to follow these
instructions verbatim with `gnd`.

Besides the tools to develop subgraphs, `gnd` also contains a version of
[graph-node](https://github.com/graphprotocol/graph-node) tailored to
running subgraphs locally via `gnd dev`.


## Getting started

Run `npm install -g @graphprotocol/gnd` to install `gnd`.

After installation, you can create and run an example subgraph locally with
```bash
gnd init --from-example ethereum-gravatar 'My new subgraph' new-subgraph
cd new-subgraph
gnd codegen
gnd build
gnd dev --ethereum-rpc 'mainnet:<RPC URL>'
```

If you have an existing contract for which you want to write a subgraph,
have a look at `gnd help init` and the `--from-contract` option. You can
also simply run `gnd init` and follow the prompts to set up your subgraph.
