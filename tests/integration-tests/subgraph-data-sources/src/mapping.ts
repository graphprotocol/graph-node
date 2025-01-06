import { Entity, log, store, BigInt, EntityTrigger, EntityOp } from '@graphprotocol/graph-ts';
import { Block } from '../generated/subgraph-Qmaqf8cRxfxbduZppSHKG9DMuX5JZPMoGuwGb2DQuo48sq';
import { MirrorBlock } from '../generated/schema';

export function handleEntity(trigger: EntityTrigger<Block>): void {
  let blockEntity = trigger.data;
  let id = blockEntity.id;

  if (trigger.operation === EntityOp.Remove) {
    log.info('Removing block entity with id: {}', [id]);
    store.remove('MirrorBlock', id);
    return;
  }

  let block = loadOrCreateMirrorBlock(id);
  block.number = blockEntity.number;
  block.hash = blockEntity.hash;
  
  if (blockEntity.testMessage) {
    block.testMessage = blockEntity.testMessage;
  }

  block.save();
}

export function loadOrCreateMirrorBlock(id: string): MirrorBlock {
  let block = MirrorBlock.load(id);
  if (!block) {
    log.info('Creating new block entity with id: {}', [id]);
    block = new MirrorBlock(id);
  }
  return block;
}
