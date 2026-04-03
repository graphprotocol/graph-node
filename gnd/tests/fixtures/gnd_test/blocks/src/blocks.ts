import { Bytes, ethereum, log } from "@graphprotocol/graph-ts";
import {Block, OnceBlock, PollingBlock} from "../generated/schema";

export function handleEveryBlock(block: ethereum.Block): void {
  log.warning("Handling every block event for block number: {}", [block.number.toString()]);
  let blockEntity = new Block(block.hash);
  blockEntity.number = block.number;
  blockEntity.save();
}

export function handleOnce(block: ethereum.Block): void {
  log.warning("Handling a one-time block event for block number: {}", [block.number.toString()]);
  let blockOnceEntity = new OnceBlock(block.hash.concat(Bytes.fromUTF8("-once")));
  blockOnceEntity.msg = "This is a one-time block entity";
  blockOnceEntity.save();
}

export function handlePolling(block: ethereum.Block): void {
  log.warning("Handling a polling block event for block number: {}", [block.number.toString()]);
  let blockEntity = new PollingBlock(block.hash);
  blockEntity.number = block.number;
  blockEntity.save();
}
