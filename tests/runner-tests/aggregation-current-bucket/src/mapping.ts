import { BigDecimal, Bytes } from "@graphprotocol/graph-ts";
import { TestEvent } from "../generated/Contract/Contract";
import { Token, Data } from "../generated/schema";

export function handleTestEvent(event: TestEvent): void {
  let command = event.params.testCommand;
  let parts = command.split(":");

  if (parts[0] == "token") {
    // Format: "token:<hex_id>:<name>"
    let token = new Token(Bytes.fromHexString(parts[1]));
    token.name = parts[2];
    token.save();
  } else if (parts[0] == "data") {
    // Format: "data:<token_hex_id>:<amount>"
    let data = new Data(0);
    data.token = Bytes.fromHexString(parts[1]);
    data.amount = BigDecimal.fromString(parts[2]);
    data.save();
  }
}
