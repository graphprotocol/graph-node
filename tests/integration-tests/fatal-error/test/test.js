const path = require("path");
const execSync = require("child_process").execSync;
const { patching } = require("gluegun");
const { createApolloFetch } = require("apollo-fetch");

const Contract = artifacts.require("./Contract.sol");

const srcDir = path.join(__dirname, "..");

const indexPort = process.env.GRAPH_NODE_INDEX_PORT || 18030;
const fetchSubgraphs = createApolloFetch({
  uri: `http://localhost:${indexPort}/graphql`,
});

const exec = (cmd) => {
  try {
    return execSync(cmd, { cwd: srcDir, stdio: "inherit" });
  } catch (e) {
    throw new Error(`Failed to run command \`${cmd}\``);
  }
};

const waitForSubgraphToFailWithError = async (blockNumber) =>
  new Promise((resolve, reject) => {
    let deadline = Date.now() + 60 * 1000;

    const checkSubgraphFailed = async () => {
      try {
        let result = await fetchSubgraphs({
          query: `{
            indexingStatusForCurrentVersion(subgraphName: "test/fatal-error") {
              health
              fatalError {
                block {
                  number
                }
                deterministic
              }

              # Test that non-fatal errors can be queried
              nonFatalErrors {
                handler
              }

              # Test that the last healthy block can be queried
              chains {
                lastHealthyBlock {
                  number
                }
              }
            }
          }`,
        });

        if (result.errors != null) {
          reject("query contains errors: " + JSON.stringify(result.errors));
        }

        let status = result.data.indexingStatusForCurrentVersion;
        if (status.health == "failed") {
          if (status.fatalError.block.number != blockNumber || status.fatalError.deterministic != true) {
            reject(
              new Error(
                "Subgraph failed with unexpected block number: " +
                  status.fatalError.block.number
              )
            );
          } else {
            resolve();
          }
        } else {
          throw new Error("reject or retry");
        }
      } catch (e) {
        if (Date.now() > deadline) {
          reject(new Error(`Timed out waiting for the subgraph to fail`));
        } else {
          setTimeout(checkSubgraphFailed, 500);
        }
      }
    };

    setTimeout(checkSubgraphFailed, 0);
  });

contract("Contract", (accounts) => {
  // Deploy the subgraph once before all tests
  before(async () => {
    // Deploy the contract
    const contract = await Contract.deployed();

    // Insert its address into subgraph manifest
    await patching.replace(
      path.join(srcDir, "subgraph.yaml"),
      "0x0000000000000000000000000000000000000000",
      contract.address
    );

    // Create and deploy the subgraph
    exec("yarn codegen");
    exec(`yarn create:test`);
    exec(`yarn deploy:test`);
  });

  it("subgraph fails with expected error", async () => {
    await waitForSubgraphToFailWithError(3);
  });
});
