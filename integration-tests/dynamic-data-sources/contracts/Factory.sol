pragma solidity ^0.4.25;

import { Exchange } from "./Exchange.sol";

contract Factory {
  address[] public exchanges;

  event NewExchange(string name, address exchange);

  constructor() public {
    createExchange("DAI");
    createExchange("GRAPH");
  }

  function createExchange(string memory name) internal {
    address exchange = address(new Exchange(name));
    exchanges.push(exchange);
    emit NewExchange(name, exchange);
  }
}
