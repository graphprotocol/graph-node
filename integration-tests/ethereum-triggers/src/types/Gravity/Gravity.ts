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

export class NewGravatar extends EthereumEvent {
  get params(): NewGravatar__Params {
    return new NewGravatar__Params(this);
  }
}

export class NewGravatar__Params {
  _event: NewGravatar;

  constructor(event: NewGravatar) {
    this._event = event;
  }

  get id(): BigInt {
    return this._event.parameters[0].value.toBigInt();
  }

  get owner(): Address {
    return this._event.parameters[1].value.toAddress();
  }

  get displayName(): string {
    return this._event.parameters[2].value.toString();
  }

  get imageUrl(): string {
    return this._event.parameters[3].value.toString();
  }
}

export class UpdatedGravatar extends EthereumEvent {
  get params(): UpdatedGravatar__Params {
    return new UpdatedGravatar__Params(this);
  }
}

export class UpdatedGravatar__Params {
  _event: UpdatedGravatar;

  constructor(event: UpdatedGravatar) {
    this._event = event;
  }

  get id(): BigInt {
    return this._event.parameters[0].value.toBigInt();
  }

  get owner(): Address {
    return this._event.parameters[1].value.toAddress();
  }

  get displayName(): string {
    return this._event.parameters[2].value.toString();
  }

  get imageUrl(): string {
    return this._event.parameters[3].value.toString();
  }
}

export class Gravity__getGravatarResult {
  value0: string;
  value1: string;

  constructor(value0: string, value1: string) {
    this.value0 = value0;
    this.value1 = value1;
  }

  toMap(): TypedMap<string, EthereumValue> {
    let map = new TypedMap<string, EthereumValue>();
    map.set("value0", EthereumValue.fromString(this.value0));
    map.set("value1", EthereumValue.fromString(this.value1));
    return map;
  }
}

export class Gravity__gravatarsResult {
  value0: Address;
  value1: string;
  value2: string;

  constructor(value0: Address, value1: string, value2: string) {
    this.value0 = value0;
    this.value1 = value1;
    this.value2 = value2;
  }

  toMap(): TypedMap<string, EthereumValue> {
    let map = new TypedMap<string, EthereumValue>();
    map.set("value0", EthereumValue.fromAddress(this.value0));
    map.set("value1", EthereumValue.fromString(this.value1));
    map.set("value2", EthereumValue.fromString(this.value2));
    return map;
  }
}

export class Gravity extends SmartContract {
  static bind(address: Address): Gravity {
    return new Gravity("Gravity", address);
  }

  getGravatar(owner: Address): Gravity__getGravatarResult {
    let result = super.call("getGravatar", [EthereumValue.fromAddress(owner)]);
    return new Gravity__getGravatarResult(
      result[0].toString(),
      result[1].toString()
    );
  }

  gravatarToOwner(param0: BigInt): Address {
    let result = super.call("gravatarToOwner", [
      EthereumValue.fromUnsignedBigInt(param0)
    ]);
    return result[0].toAddress();
  }

  ownerToGravatar(param0: Address): BigInt {
    let result = super.call("ownerToGravatar", [
      EthereumValue.fromAddress(param0)
    ]);
    return result[0].toBigInt();
  }

  gravatars(param0: BigInt): Gravity__gravatarsResult {
    let result = super.call("gravatars", [
      EthereumValue.fromUnsignedBigInt(param0)
    ]);
    return new Gravity__gravatarsResult(
      result[0].toAddress(),
      result[1].toString(),
      result[2].toString()
    );
  }
}
