import "allocator/arena";

import {Bytes, Result} from "../api_version_0_0_5/common/types";
import {debug, YAMLValue} from "../api_version_0_0_5/common/yaml";

export {memory};

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
