# Why we created the Graph

The Graph is a decentralized protocol for indexing and processing queries against data from blockchains, starting with Ethereum. It makes it possible to query for data that is difficult or impossible to query for directly.

For example, with the popular Cryptokitties dApp which implements the [ERC-721 Non-Fungible Token (NFT)](https://github.com/ethereum/eips/issues/721) standard, it is relatively straight forward to ask the following questions:
> How many cryptokitties does a specific Ethereum account own?
> When was a particular cryptokitty born?

This is because these read patterns are directly supported by the methods exposed by the [contract](https://github.com/dapperlabs/cryptokitties-bounty/blob/master/contracts/KittyCore.sol): the [`balanceOf]`(https://github.com/dapperlabs/cryptokitties-bounty/blob/master/contracts/KittyOwnership.sol#L64) and [`getKitty`](https://github.com/dapperlabs/cryptokitties-bounty/blob/master/contracts/KittyCore.sol#L91) methods, respectively.

However, other questions are more difficult to answer:
> Who are the owners of the cryptokitties born between January and February of 2018?

For this you would need to process all [`Birth` events](https://github.com/dapperlabs/cryptokitties-bounty/blob/master/contracts/KittyBase.sol#L15) and then call the [`ownerOf` method](https://github.com/dapperlabs/cryptokitties-bounty/blob/master/contracts/KittyOwnership.sol#L144) for each cryptokitty that has been born into existence. (An alternate approach could involve processing all [`Transfer` events] and filtering on the most recent transfer for each cryptokitty in existence).

The point is that even for this relatively simple question, it is already impossible for a decentralized application (dApp) running in a browser to get an answer performantly.

The way that projects solve this today is through building and hosting custom, centralized indexing and caching servers running SQL databases.

*This is the dirty secret of "decentralized" applications, as of 2018*: They almost all have significant chunks of brittle, centralized infrastructure as a part of their stack. This isn't through laziness or negligence on the part of developers, the equivalent decentralized infrastructure to support indexing and caching of blockchain data simply hasn't matured yet.

Worse yet, indexing and caching data off blockchains is hard. There are weird edge cases around finality, chain reorganizations, uncled blocks, hard forks, etc. And if each project has to reinvent the wheel with respect to indexing and caching just to build their specific dApp, the pace of dApp development in the ecosystem is likely to remain at a crawl.

The Graph solves this today by implementing an open source node implementation, [Graph Node](../README.md), which handles indexing and caching of data off blockchains, which the entire community can contribute to and leverage. It can be hosted on centralized infrastructure to provide a more robust centralized implementation of the indexing and caching infrastructure which exists today. It exposes this functionality through a tastefully designed GraphQL endpoint.

This Graph Node implementation also lays a solid foundation upon which a decentralized network of nodes (called The Graph) may handle indexing and caching - operating like a public utility upon which to build unstoppable decentralized applications in the future.