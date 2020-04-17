pragma solidity ^0.6.1;


contract Contract {
    event Trigger();

    constructor() public {
        emit Trigger();
    }
}
