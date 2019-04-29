require("babel-register");
require("babel-polyfill");
const HDWalletProvider = require("truffle-hdwallet-provider");

module.exports = {
  networks: {
    development: {
      host: "127.0.0.1",
      port: 8545,
      network_id: "*"
    },
    ropsten: {
      provider: function() {
        return new HDWalletProvider(
          process.env.MNEMONIC,
          `https://ropsten.infura.io/v3/${process.env.ROPSTEN_INFURA_API_KEY}`
        );
      },
      network_id: "3"
    },
    test: {
      host: "parity",
      port: 8545,
      network_id: "*",
      gas: "100000000000",
      gasPrice: "1",
    }
  },
  compilers: {
    solc: {
      version: "0.4.25" // Fetch exact version from solc-bin (default: truffle's version)
    }
  }
};
