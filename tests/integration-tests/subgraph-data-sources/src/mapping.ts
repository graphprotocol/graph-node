import { Entity, log } from '@graphprotocol/graph-ts';
import { MirrorBlock } from '../generated/schema';

export function handleEntity(blockEntity: Entity): void {
  let blockNumber = blockEntity.getBigInt('number');
  let blockHash = blockEntity.getBytes('hash');
  let id = blockEntity.getString('id');

  log.info('Block number: {}', [blockNumber.toString()]);

  let block = new MirrorBlock(id);
  block.number = blockNumber;
  block.hash = blockHash;
  block.save();
}
