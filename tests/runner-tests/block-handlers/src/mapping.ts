import { ethereum, log } from "@graphprotocol/graph-ts";
import { TestEvent } from "../generated/Contract/Contract";
import { Block, BlockFromOtherPollingHandler, BlockFromPollingHandler } from "../generated/schema";


export function handleBlock(block: ethereum.Block): void {
  let blockEntity = new Block(block.number.toString());
  blockEntity.number = block.number;
  blockEntity.hash = block.hash;
  blockEntity.save();
}

export function handleBlockPolling(block: ethereum.Block): void {
  let blockEntity = new BlockFromPollingHandler(block.number.toString());
  blockEntity.number = block.number;
  blockEntity.hash = block.hash;
  blockEntity.save();
}

export function handleCommand(event: TestEvent): void {
  let command = event.params.testCommand;

  if (command == "hello_world") {
    log.info("Hello World!", []);
  }
}
