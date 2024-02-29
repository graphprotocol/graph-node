// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract SimpleContract {
    event Trigger(uint16 x);

    // New event with three indexed parameters (topics)
    event AnotherTrigger(
        uint256 indexed a,
        uint256 indexed b,
        uint256 indexed c,
        string data
    );

    constructor() {
        emit Trigger(0);
    }

    function emitTrigger(uint16 x) public {
        emit Trigger(x);
    }

    // Function to emit the new event
    function emitAnotherTrigger(
        uint256 a,
        uint256 b,
        uint256 c,
        string memory data
    ) public {
        emit AnotherTrigger(a, b, c, data);
    }
}
