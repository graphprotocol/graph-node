import {debug, YAMLValue, YAMLTaggedValue} from './common/yaml';
import {Bytes, Result, TypedMap, TypedMapEntry, Wrapped} from './common/types';

enum TypeId {
    STRING = 0,
    UINT8_ARRAY = 6,

    YamlValue = 5500,
    YamlTaggedValue = 5501,
    YamlTypedMapEntryValueValue = 5502,
    YamlTypedMapValueValue = 5503,
    YamlArrayValue = 5504,
    YamlArrayTypedMapEntryValueValue = 5505,
    YamlWrappedValue = 5506,
    YamlResultValueBool = 5507,
}

export function id_of_type(type_id_index: TypeId): usize {
    switch (type_id_index) {
        case TypeId.STRING:
            return idof<string>();
        case TypeId.UINT8_ARRAY:
            return idof<Uint8Array>();

        case TypeId.YamlValue:
            return idof<YAMLValue>();
        case TypeId.YamlTaggedValue:
            return idof<YAMLTaggedValue>();
        case TypeId.YamlTypedMapEntryValueValue:
            return idof<TypedMapEntry<YAMLValue, YAMLValue>>();
        case TypeId.YamlTypedMapValueValue:
            return idof<TypedMap<YAMLValue, YAMLValue>>();
        case TypeId.YamlArrayValue:
            return idof<Array<YAMLValue>>();
        case TypeId.YamlArrayTypedMapEntryValueValue:
            return idof<Array<TypedMapEntry<YAMLValue, YAMLValue>>>();
        case TypeId.YamlWrappedValue:
            return idof<Wrapped<YAMLValue>>();
        case TypeId.YamlResultValueBool:
            return idof<Result<YAMLValue, boolean>>();
        default:
            return 0;
    }
}

export function allocate(n: usize): usize {
    return __alloc(n);
}

declare namespace yaml {
    function try_fromBytes(data: Bytes): Result<YAMLValue, boolean>;
}

export function handleYaml(data: Bytes): string {
    let result = yaml.try_fromBytes(data);

    if (result.isError) {
        return "error";
    }

    return debug(result.value);
}
