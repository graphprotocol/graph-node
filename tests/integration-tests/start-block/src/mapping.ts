import { Trigger } from "../generated/Contract/Contract";
import { Block } from "../generated/schema";

export function handleTrigger(event: Trigger): void {
  let blockHash = event.block.hash.toHex();
  let block = Block.load(blockHash);
  if (block == null) {
    block = new Block(blockHash);
  }
  block.number = event.block.number;
  block.save();
}
