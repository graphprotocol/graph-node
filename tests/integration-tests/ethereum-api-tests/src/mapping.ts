import { Address, dataSource, ethereum } from "@graphprotocol/graph-ts";
import { Trigger } from "../generated/Contract/Contract";
import { Foo } from "../generated/schema";

export function handleTrigger(event: Trigger): void {
  let entity = Foo.load("initialize");

  // The second address created from the mnemonic provided to anvil in ../../docker-compose.yml
  let address1 = Address.fromString(
    "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
  );
  let address_str = dataSource.context().getString("contract");
  let address2 = Address.fromString(address_str);

  let balance = ethereum.getBalance(address1);

  let hasCode1 = ethereum.hasCode(address1);
  let hasCode2 = ethereum.hasCode(address2);

  if (!entity) {
    entity = new Foo(event.params.x.toString());
  }

  entity.balance = balance;
  entity.hasCode1 = hasCode1.inner;
  entity.hasCode2 = hasCode2.inner;

  entity.save();
}
