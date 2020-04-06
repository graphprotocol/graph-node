pragma solidity ^0.6.1;

contract Contract {
  event Trigger();

  constructor() public {
    emit Trigger();
  }

  function exampleFunction(string memory) public pure returns (string memory) {
    return "string -> string";
  }

  function exampleFunction(uint256) public pure returns (string memory) {
    return "uint256 -> string";
  }

  function exampleFunction(bytes32) public pure returns (uint256) {
    return 256;
  }
}
