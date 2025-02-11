import {TypedMap} from './types';

export enum YAMLValueKind {
    NULL = 0,
    BOOL = 1,
    NUMBER = 2,
    STRING = 3,
    ARRAY = 4,
    OBJECT = 5,
    TAGGED = 6,
}

export class YAMLValue {
    kind: YAMLValueKind;
    data: u64;

    isBool(): boolean {
        return this.kind == YAMLValueKind.BOOL;
    }

    isNumber(): boolean {
        return this.kind == YAMLValueKind.NUMBER;
    }

    isString(): boolean {
        return this.kind == YAMLValueKind.STRING;
    }

    isArray(): boolean {
        return this.kind == YAMLValueKind.ARRAY;
    }

    isObject(): boolean {
        return this.kind == YAMLValueKind.OBJECT;
    }

    isTagged(): boolean {
        return this.kind == YAMLValueKind.TAGGED;
    }


    toBool(): boolean {
        assert(this.isBool(), 'YAML value is not a boolean');
        return this.data != 0;
    }

    toNumber(): string {
        assert(this.isNumber(), 'YAML value is not a number');
        return changetype<string>(this.data as usize);
    }

    toString(): string {
        assert(this.isString(), 'YAML value is not a string');
        return changetype<string>(this.data as usize);
    }

    toArray(): Array<YAMLValue> {
        assert(this.isArray(), 'YAML value is not an array');
        return changetype<Array<YAMLValue>>(this.data as usize);
    }

    toObject(): TypedMap<YAMLValue, YAMLValue> {
        assert(this.isObject(), 'YAML value is not an object');
        return changetype<TypedMap<YAMLValue, YAMLValue>>(this.data as usize);
    }

    toTagged(): YAMLTaggedValue {
        assert(this.isTagged(), 'YAML value is not tagged');
        return changetype<YAMLTaggedValue>(this.data as usize);
    }
}

export class YAMLTaggedValue {
    tag: string;
    value: YAMLValue;
}


export function debug(value: YAMLValue): string {
    return "(" + value.kind.toString() + ") " + debug_value(value);
}

function debug_value(value: YAMLValue): string {
    switch (value.kind) {
        case YAMLValueKind.NULL:
            return "null";
        case YAMLValueKind.BOOL:
            return value.toBool() ? "true" : "false";
        case YAMLValueKind.NUMBER:
            return value.toNumber();
        case YAMLValueKind.STRING:
            return value.toString();
        case YAMLValueKind.ARRAY: {
            let arr = value.toArray();

            let s = "[";
            for (let i = 0; i < arr.length; i++) {
                if (i > 0) {
                    s += ", ";
                }
                s += debug(arr[i]);
            }
            s += "]";

            return s;
        }
        case YAMLValueKind.OBJECT: {
            let arr = value.toObject().entries.sort((a, b) => {
                if (a.key.toString() < b.key.toString()) {
                    return -1;
                }

                if (a.key.toString() > b.key.toString()) {
                    return 1;
                }

                return 0;
            });

            let s = "{";
            for (let i = 0; i < arr.length; i++) {
                if (i > 0) {
                    s += ", ";
                }
                s += debug_value(arr[i].key) + ": " + debug(arr[i].value);
            }
            s += "}";

            return s;
        }
        case YAMLValueKind.TAGGED: {
            let tagged = value.toTagged();

            return tagged.tag + " " + debug(tagged.value);
        }
        default:
            return "undefined";
    }
}
