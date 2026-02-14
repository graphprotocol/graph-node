import { BigInt } from "@graphprotocol/graph-ts";
import { TestEvent } from "../generated/Contract/Contract";
import { Counter } from "../generated/schema";

export function handleTestEvent(event: TestEvent): void {
  let id = event.params.testCommand;

  // Create a per-event entity
  let entity = new Counter(id);
  entity.count = 1;
  entity.value = event.block.number;
  entity.label = "event_" + id;
  entity.save();

  // Read-modify-write a shared counter to exercise store.get + store.set
  let global = Counter.load("global");
  if (global == null) {
    global = new Counter("global");
    global.count = 0;
    global.value = BigInt.fromI32(0);
    global.label = "global";
  }
  global.count = global.count + 1;
  global.value = global.value.plus(BigInt.fromI32(1));
  global.save();
}
