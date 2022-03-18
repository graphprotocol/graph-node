pragma solidity ^0.8.0;


contract Contract {
    event Trigger();

    constructor() public {
    }

    function trigger() public payable {
        emit Trigger();
    }
}
