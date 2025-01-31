import { Entity, log } from '@graphprotocol/graph-ts';
import { MirrorBlock } from '../generated/schema';

export function handleEntity(blockEntity: Entity): void {
  let blockNumber = blockEntity.getBigInt('number');
  let blockHash = blockEntity.getBytes('hash');

  log.info('Block number: {}', [blockNumber.toString()]);

  let block = new MirrorBlock(blockHash);
  block.number = blockNumber;
  block.hash = blockHash;
  block.save();
}
