pragma solidity ^0.4.25;

contract Exchange {
  string public name;

  event Created(string name, address exchange);

  constructor(string memory _name) public {
    name = _name;
    emit Created(_name, address(this));
  }
}
