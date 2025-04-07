import { ethereum } from '@graphprotocol/graph-ts'
import { GraftedData } from '../generated/schema'

export function handleBlock(block: ethereum.Block): void {
  let entity = new GraftedData(block.number.toString())
  entity.data = 'from grafted'
  entity.blockNumber = block.number
  entity.save()
} 