import { ethereum, log } from "@graphprotocol/graph-ts";
import { Trigger } from "../generated/Contract/Contract";
import { Block, BlockFromOtherPollingHandler, BlockFromPollingHandler, Foo } from "../generated/schema";

export function handleBlock(block: ethereum.Block): void {
  let blockEntity = new Block(block.number.toString());
  blockEntity.number = block.number;
  blockEntity.hash = block.hash;
  blockEntity.save();
}

export function handleBlockPolling(block: ethereum.Block): void {
  log.info("handleBlockPolling {}", [block.number.toString()]);
  let blockEntity = new BlockFromPollingHandler(block.number.toString());
  blockEntity.number = block.number;
  blockEntity.hash = block.hash;
  blockEntity.save();
}

export function handleTrigger(event: Trigger): void {
  let obj = new Foo(event.params.x.toString());
  obj.value = event.params.x as i64;
  obj.save();
}
