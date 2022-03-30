## Performance Testing

This folder implements query-level performance for `graph-node`, in order to make sure PRs does not introduce performance regressions.

### How does it works?

The `k6-tests` implements an orchestrator that consists with the following:

1. Starts required Docker containers (IPFS, Ganache, PG)
2. Wait for containers to be ready 
3. Creates a new temporary DB in PG 
4. Runs `graph-node` built artifact in background
5. Use Truffle to deploy a Smart Contract, compile ABI
6. Compile Subgraph and deploy to local instance
7. Make some chain data changes to events flowing through `graph-node` and process the Subgraph
8. Execute K6 with a query, targeting the local Subgraph.
9. Run some expectations checks to make sure regressions are not introduced in PRs.
10. Reports back to GitHub as a comment.  

### Runnning locally

1. Make sure to have a clean instance of Docker running.
2. Install Node 16 and Python 3 (required for Truffle).
3. Install K6 executable (https://k6.io/open-source)
4. Build `graph-node` in release mode
5. Install Subgraph dependencies: `cd ./k6-tests/subgraph` and then `yarn install`
6. Run the following: `cargo run --package k6-tests -- --nocapture`

> Note: Running locally might have different results from CI, depends on your machine.

### Add test scenarios

If you wish to have more scenarios/checks on performance, you can change the `buildOptions` function call inside `k6-tests/k6.js`. 