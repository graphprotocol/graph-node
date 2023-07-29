const path = require("path");
const execSync = require("child_process").execSync;
const { system, patching } = require("gluegun");
const { createApolloFetch } = require("apollo-fetch");

const Web3 = require("web3");
const Contract = artifacts.require("./Contract.sol");

const srcDir = path.join(__dirname, "..");

const httpPort = process.env.GRAPH_NODE_HTTP_PORT || 18000;
const indexPort = process.env.GRAPH_NODE_INDEX_PORT || 18030;
const ganachePort = process.env.GANACHE_TEST_PORT || 18545;

const fetchSubgraphs = createApolloFetch({
  uri: `http://localhost:${indexPort}/graphql`,
});
const fetchSubgraph = createApolloFetch({
  uri: `http://localhost:${httpPort}/subgraphs/name/test/block-handlers`,
});

const exec = (cmd) => {
  try {
    return execSync(cmd, { cwd: srcDir, stdio: "inherit" });
  } catch (e) {
    throw new Error(`Failed to run command \`${cmd}\``);
  }
};

const createBlocks = async (contract) => {
  let ganacheUrl = `http://localhost:${ganachePort}`;

  const web3 = new Web3(ganacheUrl);
  let accounts = await web3.eth.getAccounts();
  // connect to the contract and call the function trigger()
  const contractInstance = new web3.eth.Contract(
    Contract.abi,
    contract.address
  );
  // loop and call emitTrigger 10 times
  // This is to force ganache to mine 10 blocks
  for (let i = 0; i < 10; i++) {
    await contractInstance.methods
      .emitTrigger(i + 1)
      .send({ from: accounts[0] });
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
          query: `{ indexingStatuses { synced, health } }`,
        });

        if (result.data.indexingStatuses[0].synced) {
          resolve();
        } else if (result.data.indexingStatuses[0].health != "healthy") {
          reject(new Error(`Subgraph failed`));
        } else {
          throw new Error("reject or retry");
        }
      } catch (e) {
        if (Date.now() > deadline) {
          reject(new Error(`Timed out waiting for the subgraph to sync`));
        } else {
          setTimeout(checkSubgraphSynced, 1000);
        }
      }
    };

    // Periodically check whether the subgraph has synced
    setTimeout(checkSubgraphSynced, 0);
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

    // We force ganache to mine atleast 10 blocks
    // by calling a function in the contract 10 times
    await createBlocks(contract);

    // Create and deploy the subgraph
    exec(`yarn codegen`);
    exec(`yarn create:test`);
    exec(`yarn deploy:test`);

    // Wait for the subgraph to be indexed
    await waitForSubgraphToBeSynced();
  });

  it("test non-filtered blockHandler", async () => {
    // Also test that multiple block constraints do not result in a graphql error.
    let result = await fetchSubgraph({
      query: `{
        blocks(orderBy: number, first: 10) { id number }
      }`,
    });
    expect(result.errors).to.be.undefined;
    expect(result.data).to.deep.equal({
      blocks: [
        { id: "1", number: "1" },
        { id: "2", number: "2" },
        { id: "3", number: "3" },
        { id: "4", number: "4" },
        { id: "5", number: "5" },
        { id: "6", number: "6" },
        { id: "7", number: "7" },
        { id: "8", number: "8" },
        { id: "9", number: "9" },
        { id: "10", number: "10" },
      ],
    });
  });

  it("test query", async () => {
    // Also test that multiple block constraints do not result in a graphql error.
    let result = await fetchSubgraph({
      query: `{
        foos(orderBy: value,skip: 1) { id value }
      }`,
    });

    expect(result.errors).to.be.undefined;
    const foos = [];
    for (let i = 0; i < 11; i++) {
      foos.push({ id: i.toString(), value: i.toString() });
    }

    expect(result.data).to.deep.equal({
      foos: foos,
    });
  });

  it("should call intialization handler first", async () => {
    let result = await fetchSubgraph({
      query: `{
        foo( id: "initialize" ) { id value }
      }`,
    });

    expect(result.errors).to.be.undefined;
    // This to test that the initialization handler is called first
    // if the value is -1 means a log handler has overwritten the value
    // meaning the initialization handler was called first
    // if the value is 0 means the log handler was called first
    expect(result.data).to.deep.equal({
      foo: { id: "initialize", value: "-1" },
    });
  });

  it("test blockHandler with polling filter", async () => {
    // Also test that multiple block constraints do not result in a graphql error.
    let result = await fetchSubgraph({
      query: `{
        blockFromPollingHandlers(orderBy: number, first: 3) { id number }
      }`,
    });
    expect(result.errors).to.be.undefined;
    expect(result.data).to.deep.equal({
      blockFromPollingHandlers: [
        { id: "1", number: "1" },
        { id: "4", number: "4" },
        { id: "7", number: "7" },
      ],
    });
  });

  it("test other blockHandler with polling filter", async () => {
    // Also test that multiple block constraints do not result in a graphql error.
    let result = await fetchSubgraph({
      query: `{
        blockFromOtherPollingHandlers(orderBy: number, first: 3) { id number }
      }`,
    });
    expect(result.errors).to.be.undefined;
    expect(result.data).to.deep.equal({
      blockFromOtherPollingHandlers: [
        { id: "2", number: "2" },
        { id: "4", number: "4" },
        { id: "6", number: "6" },
      ],
    });
  });

  it("test initialization handler", async () => {
    // Also test that multiple block constraints do not result in a graphql error.
    let result = await fetchSubgraph({
      query: `{
        initializes(orderBy: block,first:10) { id block }
      }`,
    });
    expect(result.errors).to.be.undefined;
    expect(result.data.initializes.length).to.equal(1);
    expect(result.data).to.deep.equal({
      initializes: [{ id: "1", block: "1" }],
    });
  });
});
