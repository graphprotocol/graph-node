pragma solidity >=0.8.0 <0.9.0;

contract Contract {
  event SetPurpose(address sender, string purpose);

  string public purpose = "Building Unstoppable Apps!!!";

  constructor() {
  }

  function setPurpose(string memory newPurpose) public {
      purpose = newPurpose;
      emit SetPurpose(msg.sender, purpose);
  }
}
