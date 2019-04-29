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

export class NewExchange extends EthereumEvent {
  get params(): NewExchange__Params {
    return new NewExchange__Params(this);
  }
}

export class NewExchange__Params {
  _event: NewExchange;

  constructor(event: NewExchange) {
    this._event = event;
  }

  get name(): string {
    return this._event.parameters[0].value.toString();
  }

  get exchange(): Address {
    return this._event.parameters[1].value.toAddress();
  }
}

export class Factory extends SmartContract {
  static bind(address: Address): Factory {
    return new Factory("Factory", address);
  }

  exchanges(param0: BigInt): Address {
    let result = super.call("exchanges", [
      EthereumValue.fromUnsignedBigInt(param0)
    ]);
    return result[0].toAddress();
  }
}
