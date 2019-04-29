import {
  TypedMap,
  Entity,
  Value,
  ValueKind,
  store,
  Address,
  Bytes,
  BigInt,
  BigDecimal
} from "@graphprotocol/graph-ts";

export class NewExchange extends Entity {
  constructor(id: string) {
    super();
    this.set("id", Value.fromString(id));
  }

  save(): void {
    let id = this.get("id");
    assert(id !== null, "Cannot save NewExchange entity without an ID");
    assert(
      id.kind == ValueKind.STRING,
      "Cannot save NewExchange entity with non-string ID. " +
        'Considering using .toHex() to convert the "id" to a string.'
    );
    store.set("NewExchange", id.toString(), this);
  }

  static load(id: string): NewExchange | null {
    return store.get("NewExchange", id) as NewExchange | null;
  }

  get id(): string {
    let value = this.get("id");
    return value.toString();
  }

  set id(value: string) {
    this.set("id", Value.fromString(value));
  }

  get exchange(): Bytes {
    let value = this.get("exchange");
    return value.toBytes();
  }

  set exchange(value: Bytes) {
    this.set("exchange", Value.fromBytes(value));
  }

  get name(): string {
    let value = this.get("name");
    return value.toString();
  }

  set name(value: string) {
    this.set("name", Value.fromString(value));
  }
}

export class Created extends Entity {
  constructor(id: string) {
    super();
    this.set("id", Value.fromString(id));
  }

  save(): void {
    let id = this.get("id");
    assert(id !== null, "Cannot save Created entity without an ID");
    assert(
      id.kind == ValueKind.STRING,
      "Cannot save Created entity with non-string ID. " +
        'Considering using .toHex() to convert the "id" to a string.'
    );
    store.set("Created", id.toString(), this);
  }

  static load(id: string): Created | null {
    return store.get("Created", id) as Created | null;
  }

  get id(): string {
    let value = this.get("id");
    return value.toString();
  }

  set id(value: string) {
    this.set("id", Value.fromString(value));
  }

  get exchange(): Bytes {
    let value = this.get("exchange");
    return value.toBytes();
  }

  set exchange(value: Bytes) {
    this.set("exchange", Value.fromBytes(value));
  }

  get name(): string {
    let value = this.get("name");
    return value.toString();
  }

  set name(value: string) {
    this.set("name", Value.fromString(value));
  }
}
