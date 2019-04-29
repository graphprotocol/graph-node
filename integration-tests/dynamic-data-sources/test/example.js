const { system, patching } = require("gluegun");
const path = require("path");
const { createApolloFetch } = require("apollo-fetch");

const Factory = artifacts.require("./Factory.sol");

const srcDir = path.join(__dirname, "..");

const fetchSubgraphs = createApolloFetch({
  uri: "http://graph-node:8000/subgraphs"
});

const fetchSubgraph = createApolloFetch({
  uri: "http://graph-node:8000/subgraphs/name/test/dynamic-data-sources"
});

const waitForSubgraphToBeSynced = async () => {
  let startTime = Date.now();

  while (true) {
    let now = Date.now();
    let timeout = startTime + 10000;

    if (now >= timeout) {
      throw "Timeout while waiting for the subgraph to be synced";
    }

    // Query the subgraph meta data for the indexing status
    let result = await fetchSubgraphs({
      query: `
      {
        subgraphs(where: { name: "test/dynamic-data-sources" }) {
          currentVersion {
            deployment {
              manifest { dataSources { source { address } }  }
              failed
              synced
              latestEthereumBlockNumber
              totalEthereumBlocksCount
            }
          }
        }
      }
    `
    });

    let subgraph =
      result &&
      result.data &&
      result.data.subgraphs &&
      result.data.subgraphs[0];

    let deployment =
      subgraph && subgraph.currentVersion && subgraph.currentVersion.deployment;

    if (deployment.failed) {
      throw "Indexing the subgraph failed";
    }

    if (
      deployment.synced &&
      deployment.latestEthereumBlockNumber > 0 &&
      deployment.latestEthereumBlockNumber ===
        deployment.totalEthereumBlocksCount
    ) {
      return true;
    }

    await system.run(`sleep 0.5`);
  }
};

contract("Dynamic data sources", accounts => {
  // Deploy the subgraph once before all tests
  before(async () => {
    // Deploy the contract
    const factory = await Factory.deployed();

    // Insert its address into subgraph manifest
    await patching.replace(
      path.join(srcDir, "subgraph.yaml"),
      "0x2E645469f354BB4F5c8a05B3b30A929361cf77eC",
      factory.address
    );

    // Create and deploy the subgraph
    await system.run(`yarn create-test`, { cwd: srcDir });
    await system.run(`yarn deploy-test`, { cwd: srcDir });

    // Wait for the subgraph to be indexed
    await waitForSubgraphToBeSynced();
  });

  it("4 entities are created", async () => {
    // Query the graph node for subgraph meta data
    let result = await fetchSubgraphs({
      query: `
        {
          subgraphs(where: { name: "test/dynamic-data-sources" }) {
            currentVersion {
              deployment {
                entityCount
              }
            }
          }
        }
      `
    });

    expect(result.errors).to.be.undefined;
    expect(result.data).to.deep.equal({
      subgraphs: [{ currentVersion: { deployment: { entityCount: "4" } } }]
    });
  });

  it("events from main contract and dynamic data sources are indexed", async () => {
    // Query the subgraph for avatars
    let result = await fetchSubgraph({
      query: `
        {
          newExchanges(orderBy: id) { id exchange name }
          createds(orderBy: id) { id exchange name }
        }
      `
    });

    expect(result.errors).to.be.undefined;
    expect(result.data).to.deep.equal({
      newExchanges: [
        {
          id: "0x0d370b0974454d7b0e0e3b4512c0735a6489a71a",
          exchange: "0x0d370b0974454d7b0e0e3b4512c0735a6489a71a",
          name: "GRAPH"
        },
        {
          id: "0x79183957be84c0f4da451e534d5ba5ba3fb9c696",
          exchange: "0x79183957be84c0f4da451e534d5ba5ba3fb9c696",
          name: "DAI"
        },
      ],
      createds: [
        {
          id: "0x0d370b0974454d7b0e0e3b4512c0735a6489a71a",
          exchange: "0x0d370b0974454d7b0e0e3b4512c0735a6489a71a",
          name: "GRAPH"
        },
        {
          id: "0x79183957be84c0f4da451e534d5ba5ba3fb9c696",
          exchange: "0x79183957be84c0f4da451e534d5ba5ba3fb9c696",
          name: "DAI"
        },
      ]
    });
  });
});
