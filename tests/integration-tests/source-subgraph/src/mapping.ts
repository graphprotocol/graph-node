import { ethereum, log } from '@graphprotocol/graph-ts';
import { Block } from '../generated/schema';

export function handleBlock(block: ethereum.Block): void {
  log.info('handleBlock {}', [block.number.toString()]);
  let blockEntity = new Block(block.number.toString());
  blockEntity.number = block.number;
  blockEntity.hash = block.hash;
  blockEntity.save();
}
