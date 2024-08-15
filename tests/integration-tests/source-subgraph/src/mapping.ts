import { ethereum, log } from '@graphprotocol/graph-ts';
import { Block, Block2 } from '../generated/schema';
import { BigInt } from "@graphprotocol/graph-ts";

export function handleBlock(block: ethereum.Block): void {
  log.info('handleBlock {}', [block.number.toString()]);

  let num = block.number.times(BigInt.fromI32(10)).plus(BigInt.fromI32(1));
  let blockEntity = new Block(num.toString());
  blockEntity.number = num
  block.hash[0] += 1;
  blockEntity.hash = block.hash;
  blockEntity.save();

  num = block.number.times(BigInt.fromI32(10)).plus(BigInt.fromI32(2));
  let blockEntity2 = new Block(num.toString());
  blockEntity2.number = num
  block.hash[0] += 1;
  blockEntity2.hash = block.hash;
  blockEntity2.save();

  num = block.number.times(BigInt.fromI32(10)).plus(BigInt.fromI32(3));
  let blockEntity3 = new Block2(num.toString());
  blockEntity3.number = num;
  block.hash[0] += 1;
  blockEntity3.hash2 = block.hash;
  blockEntity3.save();
}
