require("babel-register");
require("babel-polyfill");

module.exports = {
  networks: {
    test: {
      host: "localhost",
      port: 18545,
      network_id: "*",
      gas: "100000000000",
      gasPrice: "1"
    }
  },
  compilers: {
    solc: {
      version: "0.6.1"
    }
  }
};
