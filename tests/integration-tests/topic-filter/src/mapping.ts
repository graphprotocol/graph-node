import { Address, ethereum } from "@graphprotocol/graph-ts";
import { AnotherTrigger } from "../generated/Contract/Contract";
import { AnotherTriggerEntity } from "../generated/schema";

export function handleAnotherTrigger(event: AnotherTrigger): void {
  let entity = new AnotherTriggerEntity("auto");
  entity.a = event.params.a;
  entity.b = event.params.b;
  entity.c = event.params.c;
  entity.data = event.params.data;
  entity.save();
}
