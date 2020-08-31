import {
  arweave,
  box,
  json,
  Bytes,
  JSONValue,
  TypedMap,
} from "@graphprotocol/graph-ts";
import { Trigger } from "../generated/Contract/Contract";

export function handleTrigger(event: Trigger): void {
  let data = json.fromBytes(
    arweave.transactionData(
      "W2czhcswOAe4TgL4Q8kHHqoZ1jbFBntUCrtamYX_rOU"
    ) as Bytes
  );
  assert(data.toArray()[0].toString() == "Weather data for Dallas");

  let no_data = arweave.transactionData(
    "W2czhcswOAe4TgL4Q8kHHqoZ1jbFBntUCrtamYX_ZZZ"
  );
  assert(no_data === null);

  let moo_master = box.profile(
    "0xc8d807011058fcc0FB717dcd549b9ced09b53404"
  ) as TypedMap<string, JSONValue>;
  assert(moo_master.get("name").toString() == "Moo Master");

  let nothing = box.profile("0xc33307011058fcc0FB717dcd549b9ced09b53333");
  assert(nothing === null);
}
