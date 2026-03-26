import { ethereum } from "@graphprotocol/graph-ts";
import { Ping } from "../generated/schema";

export function handleBlock(block: ethereum.Block): void {
  let entity = new Ping("duplicate-entity");
  entity.value = "ping";
  entity.save();
}
