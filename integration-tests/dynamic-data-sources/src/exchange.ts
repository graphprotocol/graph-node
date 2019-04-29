import { Created as CreatedEvent } from "../generated/Factory/templates/Exchange/Exchange";
import { Created } from "../generated/schema";

export function handleCreated(event: CreatedEvent): void {
  let created = new Created(event.address.toHex());
  created.exchange = event.params.exchange;
  created.name = event.params.name;
  created.save();
}
