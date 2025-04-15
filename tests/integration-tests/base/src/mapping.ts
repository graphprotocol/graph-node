import { ethereum } from '@graphprotocol/graph-ts'
import { BaseData } from '../generated/schema'

export function handleBlock(block: ethereum.Block): void {
  let entity = new BaseData(block.number.toString())
  entity.data = 'from base'
  entity.blockNumber = block.number
  entity.save()
} 