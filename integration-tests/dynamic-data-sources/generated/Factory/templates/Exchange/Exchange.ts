import {
  EthereumEvent,
  SmartContract,
  EthereumValue,
  JSONValue,
  TypedMap,
  Entity,
  EthereumTuple,
  Bytes,
  Address,
  BigInt
} from "@graphprotocol/graph-ts";

export class Created extends EthereumEvent {
  get params(): Created__Params {
    return new Created__Params(this);
  }
}

export class Created__Params {
  _event: Created;

  constructor(event: Created) {
    this._event = event;
  }

  get name(): string {
    return this._event.parameters[0].value.toString();
  }

  get exchange(): Address {
    return this._event.parameters[1].value.toAddress();
  }
}

export class Exchange extends SmartContract {
  static bind(address: Address): Exchange {
    return new Exchange("Exchange", address);
  }

  name(): string {
    let result = super.call("name", []);
    return result[0].toString();
  }
}
