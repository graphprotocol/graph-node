import { Entity, log } from '@graphprotocol/graph-ts';

export function handleEntity(blockEntity: Entity): void {
  let blockNumberString = blockEntity.getBigInt('number').toString();
  log.info('===> Block: {}', [blockNumberString]);
}
