import { Trigger as TriggerEvent } from "../generated/Contract/Contract";
import { Trigger } from "../generated/schema";
import { log } from "@graphprotocol/graph-ts";

export function handleTrigger(event: TriggerEvent): void {
  let entity = new Trigger(event.transaction.hash.toHex());
  entity.x = event.params.x;
  entity.save();

  // Generate various log levels and types for testing
  let x = event.params.x as i32;

  if (x == 0) {
    log.info("Processing trigger with value zero", []);
  }

  if (x == 1) {
    log.error("Error processing trigger", []);
  }

  if (x == 2) {
    log.warning("Warning: unusual trigger value", ["hash", event.transaction.hash.toHexString()]);
  }

  if (x == 3) {
    log.debug("Debug: trigger details", ["blockNumber", event.block.number.toString()]);
  }

  if (x == 4) {
    log.info("Handler execution successful", ["entity_id", entity.id]);
  }

  if (x == 5) {
    log.error("Critical timeout error", []);
  }

  // Log for every event to test general log capture
  log.info("Trigger event processed", []);
}
