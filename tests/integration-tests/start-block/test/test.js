const path = require("path");
const execSync = require("child_process").execSync;
const { system, patching } = require("gluegun");
const { createApolloFetch } = require("apollo-fetch");

const Contract = artifacts.require("./Contract.sol");

const srcDir = path.join(__dirname, "..");

const httpPort = process.env.GRAPH_NODE_HTTP_PORT || 18000;
const indexPort = process.env.GRAPH_NODE_INDEX_PORT || 18030;

const fetchSubgraphs = createApolloFetch({
  uri: `http://localhost:${indexPort}/graphql`
});
const fetchSubgraph = createApolloFetch({
  uri: `http://localhost:${httpPort}/subgraphs/name/test/start-block`
});

const exec = cmd => {
  try {
    return execSync(cmd, { cwd: srcDir, stdio: "inherit" });
  } catch (e) {
    throw new Error(`Failed to run command \`${cmd}\``);
  }
};

const waitForSubgraphToBeSynced = async () =>
  new Promise((resolve, reject) => {
    // Wait for 60s
    let deadline = Date.now() + 60 * 1000;

    // Function to check if the subgraph is synced
    const checkSubgraphSynced = async () => {
      try {
        let result = await fetchSubgraphs({
          query: `{ indexingStatuses { synced, health } }`
        });

        if (result.data.indexingStatuses[0].synced) {
          resolve();
        } else if (result.data.indexingStatuses[0].health != "healthy") {
          reject(new Error("Subgraph failed"));
        } else {
          throw new Error("reject or retry");
        }
      } catch (e) {
        if (Date.now() > deadline) {
          reject(new Error(`Timed out waiting for the subgraph to sync`));
        } else {
          setTimeout(checkSubgraphSynced, 500);
        }
      }
    };

    // Periodically check whether the subgraph has synced
    setTimeout(checkSubgraphSynced, 0);
  });

const queryProcessedBlocks = async () =>
  fetchSubgraph({
    query: `{ blocks(orderBy: number, orderDirection: asc) { number } }`
  });

contract("Contract", accounts => {
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

  it("subgraph does not fail", async () => {
    // Wait for the subgraph to be indexed, and not fail
    await waitForSubgraphToBeSynced();

    let blocks = await queryProcessedBlocks();

    // The subgraph start block is 15; for details on the blocks
    // generated, see also: e294e89e-7409-4ec1-a137-0b25f4f35899

    expect(blocks.data.blocks).to.deep.equal([
      { number: "15" },
      { number: "16" },
      { number: "17" },
      { number: "18" },
      { number: "19" },
      { number: "20" },
      { number: "21" },
      { number: "22" },
      { number: "23" },
      { number: "24" }
    ]);
  });
});
