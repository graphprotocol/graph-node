import { BigInt, ethereum } from '@graphprotocol/graph-ts'
import { LSData } from '../generated/schema'

export function handleBlock(block: ethereum.Block): void {
  let diff = 1
  let entity = new LSData(block.number.toString())
  entity.data = 'original entity'
  entity.blockNumber = block.number
  entity.save()
  let block_number = block.number
  if (block_number.gt(BigInt.fromI32(diff))) {
    let bn = block_number.minus(BigInt.fromI32(diff)).toString()
    let entity2 = LSData.load(bn)
    if (entity2 != null) {
      entity2.data = 'modified entity'
      entity2.save()
    }
  }
}
