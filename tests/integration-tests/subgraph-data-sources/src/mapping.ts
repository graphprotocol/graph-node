import { Entity, log } from '@graphprotocol/graph-ts';
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
  let id = blockEntity.getString('id');

  log.info('Block number: {}', [blockNumber.toString()]);

  let block = new MirrorBlock(id);
  block.number = blockNumber;
  block.hash = blockHash;
  block.save();
}
