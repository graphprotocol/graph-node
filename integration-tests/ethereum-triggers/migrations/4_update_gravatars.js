const GravatarRegistry = artifacts.require('./GravatarRegistry.sol')

module.exports = async function(deployer) {
  let accounts = await web3.eth.getAccounts()

  const registry = await GravatarRegistry.deployed()
  await registry.updateGravatarName('Nena', { from: accounts[0] })
  await registry.updateGravatarName('Jorge', { from: accounts[1] })
}
