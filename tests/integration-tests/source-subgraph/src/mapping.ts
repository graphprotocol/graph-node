import { ethereum, log, store } from '@graphprotocol/graph-ts';
import { Block, Block2 } from '../generated/schema';
import { BigInt } from '@graphprotocol/graph-ts';

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
  blockEntity3.save();

  if (block.number.equals(BigInt.fromI32(1))) {
    let id = 'TEST';
    let entity = new Block(id);
    entity.number = block.number;
    entity.hash = block.hash;
    entity.testMessage = 'Created at block 1';
    log.info('Created entity at block 1', []);
    entity.save();
  }

  if (block.number.equals(BigInt.fromI32(2))) {
    let id = 'TEST';
    let blockEntity1 = Block.load(id);
    if (blockEntity1) {
      // Update the block entity
      blockEntity1.testMessage = 'Updated at block 2';
      log.info('Updated entity at block 2', []);
      blockEntity1.save();
    }
  }

  if (block.number.equals(BigInt.fromI32(3))) {
    let id = 'TEST';
    let blockEntity1 = Block.load(id);
    if (blockEntity1) {
      blockEntity1.testMessage = 'Deleted at block 3';
      log.info('Deleted entity at block 3', []);
      blockEntity1.save();
      store.remove('Block', id);
    }
  }
}
