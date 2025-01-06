import { ethereum } from '@graphprotocol/graph-ts'
import { SourceBData } from '../generated/schema'

export function handleBlock(block: ethereum.Block): void {
  let entity = new SourceBData(block.number.toString())
  entity.data = 'from source B'
  entity.blockNumber = block.number
  entity.save()
} 