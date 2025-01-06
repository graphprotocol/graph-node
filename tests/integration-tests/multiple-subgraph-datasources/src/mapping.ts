import { dataSource, EntityTrigger } from '@graphprotocol/graph-ts'
import { AggregatedData } from '../generated/schema'
import { SourceAData } from '../generated/subgraph-QmU35YwAsv59gJxhejp3qqUrSFMaoBXuskNLX7wJHNUzyA'
import { SourceBData } from '../generated/subgraph-QmXjHeC7j5iWF49oEngV3tqFrHvb4NhkFpAdJYVJ1SFPNk'

export function handleSourceAData(data: EntityTrigger<SourceAData>): void {
  let aggregated = AggregatedData.load('1')
  if (!aggregated) {
    aggregated = new AggregatedData('1')
    aggregated.sourceA = data.data.data
  } else {
    aggregated.sourceA = data.data.data
  }
  aggregated.save()
}

export function handleSourceBData(data: EntityTrigger<SourceBData>): void {
  let aggregated = AggregatedData.load('1')
  if (!aggregated) {
    aggregated = new AggregatedData('1')
    aggregated.sourceB = data.data.data
  } else {
    aggregated.sourceB = data.data.data
  }
  aggregated.save()
}
