const fs = require("fs");

async function main() {
  const { abi } = require("./truffle_output/Contract.json");

  try {
    fs.mkdirSync("./abis");
  } catch (e) {}

  fs.writeFileSync("./abis/Contract.json", JSON.stringify(abi, null, 2));
}

main().catch(console.error);
