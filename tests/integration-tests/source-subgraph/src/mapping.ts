import { ethereum, log, store } from '@graphprotocol/graph-ts';
import { Block, Block2, Block3 } from '../generated/schema';

export function handleBlock(block: ethereum.Block): void {
  log.info('handleBlock {}', [block.number.toString()]);

  let id = block.number.toString().concat('-v1');
  let blockEntity = new Block(id);
  blockEntity.number = block.number;
  blockEntity.hash = block.hash;
  blockEntity.save();

  let id2 = block.number.toString().concat('-v2');
  let blockEntity2 = new Block(id2);
  blockEntity2.number = block.number;
  blockEntity2.hash = block.hash;
  blockEntity2.save();

  let id3 = block.number.toString().concat('-v3');
  let blockEntity3 = new Block2(id3);
  blockEntity3.number = block.number;
  blockEntity3.hash = block.hash;
  blockEntity3.testMessage = block.number.toString().concat('-message');
  blockEntity3.save();

  let id4 = block.hash;
  let blockEntity4 = new Block3(id4);
  blockEntity4.number = block.number;
  blockEntity4.testMessage = block.number.toString().concat('-message');
  blockEntity4.save();
}
