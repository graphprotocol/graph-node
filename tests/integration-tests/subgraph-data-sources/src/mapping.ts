import { log, store } from '@graphprotocol/graph-ts';
import { Block, Block2 } from '../generated/subgraph-QmWi3H11QFE2PiWx6WcQkZYZdA5UasaBptUJqGn54MFux5';
import { MirrorBlock } from '../generated/schema';

export function handleEntity(block: Block): void {
  let id = block.id;

  let blockEntity = loadOrCreateMirrorBlock(id);
  blockEntity.number = block.number;
  blockEntity.hash = block.hash;

  blockEntity.save();
}

export function handleEntity2(block: Block2): void {
  let id = block.id;

  let blockEntity = loadOrCreateMirrorBlock(id);
  blockEntity.number = block.number;
  blockEntity.hash = block.hash;
  blockEntity.testMessage = block.testMessage;

  blockEntity.save();
}

export function loadOrCreateMirrorBlock(id: string): MirrorBlock {
  let block = MirrorBlock.load(id);
  if (!block) {
    log.info('Creating new block entity with id: {}', [id]);
    block = new MirrorBlock(id);
  }
  return block;
}
