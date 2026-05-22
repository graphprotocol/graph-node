import { log, store } from '@graphprotocol/graph-ts';
import { Block, Block2, Block3 } from '../generated/subgraph-Contract';
import { MirrorBlock, MirrorBlockBytes } from '../generated/schema';

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

export function handleEntity3(block: Block3): void {
  let id = block.id;

  let blockEntity = new MirrorBlockBytes(id);
  blockEntity.number = block.number;
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
