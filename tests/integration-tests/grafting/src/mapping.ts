import { ethereum } from '@graphprotocol/graph-ts'
import { GraftingData } from '../generated/schema'

export function handleBlock(block: ethereum.Block): void {
  let entity = new GraftingData(block.number.toString())
  entity.data = 'to grafting'
  entity.blockNumber = block.number
  entity.save()
} 