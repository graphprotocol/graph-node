import { DataSourceContext, Address } from "@graphprotocol/graph-ts";
import { Trigger } from "../generated/Contract/Contract";
import { Dynamic } from "../generated/templates";

export function handleTrigger(event: Trigger): void {
  let context = new DataSourceContext();
  context.setI32("answer", 42);
  Dynamic.createWithContext(
    changetype<Address>(Address.fromHexString(
      "0x2E645469f354BB4F5c8a05B3b30A929361cf77eC"
    )),
    context
  );
}
