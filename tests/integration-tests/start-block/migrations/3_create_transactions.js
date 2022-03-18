const Contract = artifacts.require("./Contract.sol");

module.exports = async function(deployer) {
  const instance = await Contract.deployed();

  // See also: e294e89e-7409-4ec1-a137-0b25f4f35899

  for (let i = 0; i < 20; i++) {
    // Generates block #(5+i)
    await instance.trigger();
  }
};
