// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract DeclaredCallsContract {
    // Asset struct for testing struct field access
    struct Asset {
        address addr; // field 0
        uint256 amount; // field 1
        bool active; // field 2
    }

    // Complex nested struct for advanced testing
    struct ComplexAsset {
        Asset base; // field 0
        string metadata; // field 1
        uint256[] values; // field 2
    }

    // Events for testing declared calls
    event Transfer(address indexed from, address indexed to, uint256 value);
    event AssetTransfer(Asset asset, address to, uint256 blockNumber);
    event ComplexAssetCreated(ComplexAsset complexAsset, uint256 id);

    // Storage for testing view functions
    mapping(address => uint256) public balances;
    mapping(address => string) public metadata;
    mapping(uint256 => Asset) public assets;
    mapping(uint256 => address) public assetOwners;
    uint256 public totalSupply;

    // State variables for testing
    bool public shouldRevert = false;
    uint256 public counter = 0;

    constructor() {
        // Initialize some test data
        balances[msg.sender] = 1000;
        balances[address(0x1111111111111111111111111111111111111111)] = 1000;
        balances[address(0x2222222222222222222222222222222222222222)] = 1000;
        totalSupply = 3000;

        // Create some test assets
        assets[1] = Asset({
            addr: address(0x1111111111111111111111111111111111111111),
            amount: 100,
            active: true
        });
        assetOwners[1] = msg.sender;
        metadata[
            address(0x1111111111111111111111111111111111111111)
        ] = "Test Asset 1";

        assets[2] = Asset({
            addr: address(0x2222222222222222222222222222222222222222),
            amount: 200,
            active: false
        });
        assetOwners[2] = msg.sender;
        metadata[
            address(0x2222222222222222222222222222222222222222)
        ] = "Test Asset 2";
    }

    // Basic functions for declared calls testing
    function balanceOf(address account) public view returns (uint256) {
        return balances[account];
    }

    function getOwner(address assetAddr) public view returns (address) {
        // Find asset by address and return owner
        for (uint256 i = 1; i <= 10; i++) {
            if (assets[i].addr == assetAddr) {
                return assetOwners[i];
            }
        }
        return address(0);
    }

    function getMetadata(
        address assetAddr
    ) public view returns (string memory) {
        return metadata[assetAddr];
    }

    function getAssetAmount(uint256 assetId) public view returns (uint256) {
        return assets[assetId].amount;
    }

    function isAssetActive(uint256 assetId) public view returns (bool) {
        return assets[assetId].active;
    }

    // Functions for testing edge cases
    function alwaysReverts() public pure returns (bool) {
        if (1 > 0) {
            revert("This function always reverts");
        }
        return true;
    }

    function conditionalRevert() public view {
        if (shouldRevert) {
            revert("Conditional revert triggered");
        }
    }

    function incrementCounter() public returns (uint256) {
        counter++;
        return counter;
    }

    // Functions to emit events for testing
    function emitTransfer(address from, address to, uint256 value) public {
        balances[from] -= value;
        balances[to] += value;
        emit Transfer(from, to, value);
    }

    function emitAssetTransfer(
        address assetAddr,
        uint256 amount,
        bool active,
        address to
    ) public {
        Asset memory asset = Asset({
            addr: assetAddr,
            amount: amount,
            active: active
        });
        emit AssetTransfer(asset, to, block.number);
    }

    function emitComplexAssetCreated(
        address baseAddr,
        uint256 baseAmount,
        bool baseActive,
        string memory metadataStr,
        uint256[] memory values,
        uint256 id
    ) public {
        Asset memory baseAsset = Asset({
            addr: baseAddr,
            amount: baseAmount,
            active: baseActive
        });

        ComplexAsset memory complexAsset = ComplexAsset({
            base: baseAsset,
            metadata: metadataStr,
            values: values
        });

        emit ComplexAssetCreated(complexAsset, id);
    }

    // Utility functions
    function setShouldRevert(bool _shouldRevert) public {
        shouldRevert = _shouldRevert;
    }

    function getConstant() public pure returns (uint256) {
        return 42;
    }

    function sum(uint256 a, uint256 b) public pure returns (uint256) {
        return a + b;
    }

    // Function that doesn't exist in ABI (for testing invalid function calls)
    // This will be removed from the ABI manually
    function hiddenFunction() public pure returns (uint256) {
        return 999;
    }
}
