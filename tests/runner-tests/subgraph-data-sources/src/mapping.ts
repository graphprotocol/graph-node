import { Entity, log } from '@graphprotocol/graph-ts';

export function handleBlock(content: Entity): void {
  let stringContent = content.getString('val');
  log.info('Content: {}', [stringContent]);
}
