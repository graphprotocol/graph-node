const path = require("path");
const { execSync } = require("child_process");
const { patching } = require("gluegun");
const { GraphQLClient } = require("graphql-request");
const { copyFileSync } = require("fs");
const Contract = artifacts.require("./contract/Contract.sol");

const indexPort = process.env.GRAPH_NODE_INDEX_PORT || 18030;
const client = new GraphQLClient(`http://localhost:${indexPort}/graphql`);

const exec = (cmd) => {
  try {
    return execSync(cmd, { cwd: "./", stdio: "inherit" });
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
      console.log(`Checking Subgraph sync state...`);
      try {
        let result = await client.request(
          `{ indexingStatuses { synced, health } }`,
          {}
        );
        console.log(`indexingStatuses response:`, result);

        if (result.indexingStatuses[0].synced) {
          console.log(`Subgraph is now in sync!`);
          resolve();
        } else if (result.indexingStatuses[0].health != "healthy") {
          console.log(`Not healthy!`);
          reject(new Error("Subgraph failed"));
        } else {
          console.log(`unexpected result, will trying again`);
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

contract("Contract", (accounts) => {
  // Deploy the subgraph once before all tests
  before(async () => {
    console.log(`Deploying Contract...`);
    // Deploy the contract
    const contract = await Contract.deployed();
    console.log(
      `Done, creating subgraph.yaml from template, based on contract address`
    );

    console.log(`Creating some blocks...`);
    copyFileSync("subgraph.template.yaml", "subgraph.yaml");

    // Insert its address into subgraph manifest
    await patching.replace("subgraph.yaml", "{ADDRESS}", contract.address);

    // Create and deploy the subgraph
    exec(`yarn codegen`);
    exec(`yarn create:test`);
    exec(`yarn deploy:test`);

    // Inject some data to the contract
    await contract.setPurpose("test 1");
    await contract.setPurpose("test 2");
    await contract.setPurpose("test 3");
  });

  it("Subgraph should be in sync", async () => {
    // Wait for the subgraph to be indexed, and not fail
    await waitForSubgraphToBeSynced();
  });
});
