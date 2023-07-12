import { Address, ethereum, log } from "@graphprotocol/graph-ts";
import { Contract, Trigger } from "../generated/Contract/Contract";
import { Block, BlockFromOtherPollingHandler, BlockFromPollingHandler, Foo } from "../generated/schema";
import { ContractTemplate } from "../../../runner-tests/block-handlers/generated/templates";

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
export function handleBlockPollingFromTemplate(block: ethereum.Block): void {
  log.info("===> handleBlockPollingFromTemplate {}", [block.number.toString()]);
  let blockEntity = new BlockFromOtherPollingHandler(block.number.toString());
  blockEntity.number = block.number;
  blockEntity.hash = block.hash;
  blockEntity.save();
}

export function handleTrigger(event: Trigger): void {
  let obj = new Foo(event.params.x.toString());
  obj.value = event.params.x as i64;
  obj.save();
   
  if(event.params.x == 0){
    log.info("===> Creating template {}", [ Address.fromString("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48").toHexString()]);
    ContractTemplate.create( Address.fromString("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"));
  }
}
