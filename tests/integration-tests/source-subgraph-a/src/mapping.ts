import { ethereum } from '@graphprotocol/graph-ts'
import { SourceAData } from '../generated/schema'

export function handleBlock(block: ethereum.Block): void {
  let entity = new SourceAData('1')
  entity.data = 'from source A'
  entity.timestamp = block.timestamp
  entity.save()
} 