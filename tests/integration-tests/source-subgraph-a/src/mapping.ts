import { ethereum } from '@graphprotocol/graph-ts'
import { SourceAData } from '../generated/schema'

export function handleBlock(block: ethereum.Block): void {
  let entity = new SourceAData(block.number.toString())
  entity.data = 'from source A'
  entity.blockNumber = block.number
  entity.save()
} 