import { Entity, log, store } from '@graphprotocol/graph-ts';
import { MirrorBlock } from '../generated/schema';

export class EntityTrigger {
  constructor(
    public entityOp: u32,
    public entityType: string,
    public entity: Entity,
    public vid: i64,
  ) {}
}

export function handleEntity(trigger: EntityTrigger): void {
  let blockEntity = trigger.entity;
  let blockNumber = blockEntity.getBigInt('number');
  let blockHash = blockEntity.getBytes('hash');
  let testMessage = blockEntity.get('testMessage');
  let id = blockEntity.getString('id');

  log.info('Block number: {}', [blockNumber.toString()]);

  if (trigger.entityOp == 2) {
    log.info('Removing block entity with id: {}', [id]);
    store.remove('MirrorBlock', id);
    return;
  }

  let block = loadOrCreateMirrorBlock(id);
  block.number = blockNumber;
  block.hash = blockHash;
  if (testMessage) {
    block.testMessage = testMessage.toString();
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
