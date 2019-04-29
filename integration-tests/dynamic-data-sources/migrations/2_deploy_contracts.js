const Factory = artifacts.require('./Factory.sol')

module.exports = async function(deployer) {
  await deployer.deploy(Factory)
}
