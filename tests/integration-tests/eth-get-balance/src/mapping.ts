import { Address, ethereum } from "@graphprotocol/graph-ts";
import { Trigger } from "../generated/Contract/Contract";
import { Foo } from "../generated/schema";

export function handleTrigger(event: Trigger): void {
  let entity = Foo.load("initialize");

  let balance = ethereum.getBalance(
    // The second address created from the mnemonic provided to anvil in ../../docker-compose.yml
    Address.fromString("0x70997970C51812dc3A010C7d01b50e0d17dc79C8"),
  );

  if (!entity) {
    entity = new Foo(event.params.x.toString());
    entity.value = balance
  } else {
    entity.value = balance
  }

  entity.save();
}
