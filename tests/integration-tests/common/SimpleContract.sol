pragma solidity ^0.6.1;


contract Contract {
    event Trigger(uint16 x);

    constructor() public {
        emit Trigger(0);
    }

    function emitTrigger(uint16 x) public {
        emit Trigger(x);
    }
}
