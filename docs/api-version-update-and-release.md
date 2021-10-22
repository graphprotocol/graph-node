# `apiVersion` Upgrade and Release Guide

This document explains how to coordenate an `apiVersion` upgrade
across all impacted projects:

1.  [`graph-node`](https:github.com/graphprotocol/graph-node)
2.  [`graph-ts`](https:github.com/graphprotocol/graph-ts)
3.  [`graph-cli`](https:github.com/graphprotocol/graph-cli)
4.  [`graph-docs`](https:github.com/graphprotocol/graph-docs)

## Steps

Those steps should be taken after all relevant `graph-node` changes
have been rolled out to production:

1.  Update the default value of `MAX_API_VERSION` constant located in
    the `graph/src/data/subgraph/mod.rs` file. If the
    `GRAPH_MAX_API_VERSION` environment variable is being used, update
    it to the same version as well.

2.  Update `graph-node` minor version and create a new release.

3.  Update `graph-ts` version and create a new release.

4.  For `graph-cli`:

    1.  Write migrations for the new `apiVersion`.
    2.  Update the version restriction on the `build` and `deploy`
        commands to match the new `gaph-ts` and `apiVersion` versions.
    3.  Update the `graph-ts` version in `package.json`.
    4.  Update the `graph-ts` version on scaffolded code and examples.
    5.  Recompile all the examples by running `$ yarn install` inside
        each example directory.
    6.  Update `graph-cli`\'s version and create a new release.
    7.  Release in NPM

5.  Update `graph-docs` with the new `apiVersion` content.
