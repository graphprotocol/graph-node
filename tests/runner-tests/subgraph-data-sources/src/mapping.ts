import { Entity, log } from '@graphprotocol/graph-ts';

export const SubgraphEntityOpCreate: u32 = 0;
export const SubgraphEntityOpModify: u32 = 1;
export const SubgraphEntityOpDelete: u32 = 2;

export class EntityTrigger {
  constructor(
    public entityOp: u32,
    public entityType: string,
    public entity: Entity,
    public vid: i64,
  ) {}
}

export function handleBlock(content: EntityTrigger): void {
  let stringContent = content.entity.getString('val');
  log.info('Content: {}', [stringContent]);
  log.info('EntityOp: {}', [content.entityOp.toString()]);

  switch (content.entityOp) {
    case SubgraphEntityOpCreate: {
      log.info('Entity created: {}', [content.entityType]);
      break
    }
    case SubgraphEntityOpModify: {
      log.info('Entity modified: {}', [content.entityType]);
      break;
    }
    case SubgraphEntityOpDelete: {
      log.info('Entity deleted: {}', [content.entityType]);
      break;
    }
  }
}
