import { Address, dataSource, ethereum } from "@graphprotocol/graph-ts";
import { Trigger } from "../generated/Contract/Contract";
import { Foo } from "../generated/schema";

export function handleTrigger(event: Trigger): void {
  let entity = Foo.load("initialize");

  let balance = ethereum.getBalance(
    // The second address created from the mnemonic provided to anvil in ../../docker-compose.yml
    Address.fromString("0x70997970C51812dc3A010C7d01b50e0d17dc79C8"),
  );

  let address_str = dataSource.context().getString("contract");
  let address = Address.fromString(address_str);

  let code = ethereum.getCode(address);

  if (!entity) {
    entity = new Foo(event.params.x.toString());
  }

  entity.balance = balance;
  entity.code = code;

  entity.save();
}
