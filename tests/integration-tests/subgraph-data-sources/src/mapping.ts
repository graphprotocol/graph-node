import { Entity, log } from '@graphprotocol/graph-ts';
import { Block } from '../generated/schema';

export function handleEntity(blockEntity: Entity): void {
  let blockNumber = blockEntity.getBigInt('number');
  let blockHash = blockEntity.getBytes('hash');

  log.info('Block number: {}', [blockNumber.toString()]);

  let block = new Block(blockHash);
  block.number = blockNumber;
  block.hash = blockHash;
  block.save();
}
