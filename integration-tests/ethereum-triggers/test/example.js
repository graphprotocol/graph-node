const { system, patching } = require("gluegun");
const path = require("path");
const { createApolloFetch } = require("apollo-fetch");

const GravatarRegistry = artifacts.require("./GravatarRegistry.sol");

const srcDir = path.join(__dirname, "..");

const fetchSubgraphs = createApolloFetch({
  uri: "http://graph-node:8000/subgraphs"
});

const fetchSubgraph = createApolloFetch({
  uri: "http://graph-node:8000/subgraphs/name/test/ethereum-triggers"
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
        subgraphs(where: { name: "test/ethereum-triggers" }) {
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
}

contract("Ethereum triggers", accounts => {
  // Deploy the subgraph once before all tests
  before(async () => {
    // Deploy the contract
    const registry = await GravatarRegistry.deployed();

    // Insert its address into subgraph manifest
    await patching.replace(
      path.join(srcDir, "subgraph.yaml"),
      "0x2E645469f354BB4F5c8a05B3b30A929361cf77eC",
      registry.address
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
          subgraphs(where: { name: "test/ethereum-triggers" }) {
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

  it("all events are indexed", async () => {
    // Query the subgraph for avatars
    let result = await fetchSubgraph({
      query: `
        {
          newGravatars(orderBy: id) { id displayName imageUrl }
          updatedGravatars(orderBy: id) { id displayName imageUrl }
        }
      `
    });

    expect(result.errors).to.be.undefined;
    expect(result.data).to.deep.equal({
      newGravatars: [
        {
          displayName: "Carl",
          id: "0x1",
          imageUrl: "https://thegraph.com/img/team/team_04.png"
        },
        {
          displayName: "Lucas",
          id: "0x2",
          imageUrl: "https://thegraph.com/img/team/bw_Lucas.jpg"
        }
      ],
      updatedGravatars: [
        {
          displayName: "Nena",
          id: "0x1",
          imageUrl: "https://thegraph.com/img/team/team_04.png"
        },
        {
          displayName: "Jorge",
          id: "0x2",
          imageUrl: "https://thegraph.com/img/team/bw_Lucas.jpg"
        }
      ],
    });
  });
});
