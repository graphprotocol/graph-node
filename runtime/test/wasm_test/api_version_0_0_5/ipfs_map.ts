enum IndexForAscTypeId {
  STRING = 0,
  ARRAY_BUFFER = 1,
  UINT8_ARRAY = 6,
  ARRAY_TYPED_MAP_ENTRY_STRING_JSON_VALUE = 20,
  ARRAY_TYPED_MAP_ENTRY_STRING_STORE_VALUE = 21,
  STORE_VALUE = 31,
  JSON_VALUE = 32,
  TYPED_MAP_ENTRY_STRING_STORE_VALUE = 34,
  TYPED_MAP_ENTRY_STRING_JSON_VALUE = 35,
  TYPED_MAP_STRING_STORE_VALUE = 36,
  TYPED_MAP_STRING_JSON_VALUE = 37,
}

export function id_of_type(type_id_index: IndexForAscTypeId): usize {
  switch (type_id_index) {
    case IndexForAscTypeId.STRING:
      return idof<string>();
    case IndexForAscTypeId.ARRAY_BUFFER:
      return idof<ArrayBuffer>();
    case IndexForAscTypeId.UINT8_ARRAY:
      return idof<Uint8Array>();
    case IndexForAscTypeId.ARRAY_TYPED_MAP_ENTRY_STRING_JSON_VALUE:
      return idof<Array<TypedMapEntry<string, JSONValue>>>();
    case IndexForAscTypeId.ARRAY_TYPED_MAP_ENTRY_STRING_STORE_VALUE:
      return idof<Array<Entity>>();
    case IndexForAscTypeId.STORE_VALUE:
      return idof<Value>();
    case IndexForAscTypeId.JSON_VALUE:
      return idof<JSONValue>();
    case IndexForAscTypeId.TYPED_MAP_ENTRY_STRING_STORE_VALUE:
      return idof<Entity>();
    case IndexForAscTypeId.TYPED_MAP_ENTRY_STRING_JSON_VALUE:
      return idof<TypedMapEntry<string, JSONValue>>();
    case IndexForAscTypeId.TYPED_MAP_STRING_STORE_VALUE:
      return idof<TypedMap<string, Value>>();
    case IndexForAscTypeId.TYPED_MAP_STRING_JSON_VALUE:
      return idof<TypedMap<string, JSONValue>>();
    default:
      return 0;
  }
}

export function allocate(n: usize): usize {
  return __alloc(n);
}

/*
 * Declarations copied from graph-ts/input.ts and edited for brevity
 */

declare namespace store {
  function set(entity: string, id: string, data: Entity): void
}

class TypedMapEntry<K, V> {
  key: K
  value: V

  constructor(key: K, value: V) {
    this.key = key
    this.value = value
  }
}

class TypedMap<K, V> {
  entries: Array<TypedMapEntry<K, V>>

  constructor() {
    this.entries = new Array<TypedMapEntry<K, V>>(0)
  }

  set(key: K, value: V): void {
    let entry = this.getEntry(key)
    if (entry !== null) {
      entry.value = value
    } else {
      let entry = new TypedMapEntry<K, V>(key, value)
      this.entries.push(entry)
    }
  }

  getEntry(key: K): TypedMapEntry<K, V> | null {
    for (let i: i32 = 0; i < this.entries.length; i++) {
      if (this.entries[i].key == key) {
        return this.entries[i]
      }
    }
    return null
  }

  get(key: K): V | null {
    for (let i: i32 = 0; i < this.entries.length; i++) {
      if (this.entries[i].key == key) {
        return this.entries[i].value
      }
    }
    return null
  }
}

enum ValueKind {
  STRING = 0,
  INT = 1,
  FLOAT = 2,
  BOOL = 3,
  ARRAY = 4,
  NULL = 5,
  BYTES = 6,
  BIGINT = 7,
}

type ValuePayload = u64

class Value {
  kind: ValueKind
  data: ValuePayload

  toString(): string {
    assert(this.kind == ValueKind.STRING, 'Value is not a string.')
    return changetype<string>(this.data as u32)
  }

  static fromString(s: string): Value {
    let value = new Value()
    value.kind = ValueKind.STRING
    value.data = changetype<u32>(s)
    return value
  }
}

class Entity extends TypedMap<string, Value> { }

enum JSONValueKind {
  NULL = 0,
  BOOL = 1,
  NUMBER = 2,
  STRING = 3,
  ARRAY = 4,
  OBJECT = 5,
}

type JSONValuePayload = u64
class JSONValue {
  kind: JSONValueKind
  data: JSONValuePayload

  toString(): string {
    assert(this.kind == JSONValueKind.STRING, 'JSON value is not a string.')
    return changetype<string>(this.data as u32)
  }

  toObject(): TypedMap<string, JSONValue> {
    assert(this.kind == JSONValueKind.OBJECT, 'JSON value is not an object.')
    return changetype<TypedMap<string, JSONValue>>(this.data as u32)
  }
}

/*
 * Actual setup for the test
 */
declare namespace ipfs {
  function map(hash: String, callback: String, userData: Value, flags: String[]): void
}

export function echoToStore(data: JSONValue, userData: Value): void {
  // expect a map of the form { "id": "anId", "value": "aValue" }
  let map = data.toObject();

  let id = map.get("id");
  let value = map.get("value");

  assert(id !== null, "'id' should not be null");
  assert(value !== null, "'value' should not be null");

  let stringId = id!.toString();
  let stringValue = value!.toString();

  let entity = new Entity();
  entity.set("id", Value.fromString(stringId));
  entity.set("value", Value.fromString(stringValue));
  entity.set("extra", userData);
  store.set("Thing", stringId, entity);
}

export function ipfsMap(hash: string, userData: string): void {
  ipfs.map(hash, "echoToStore", Value.fromString(userData), ["json"])
}
