const GravatarRegistry = artifacts.require('./GravatarRegistry.sol')

module.exports = async function(deployer) {
  await deployer.deploy(GravatarRegistry)
}
