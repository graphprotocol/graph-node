import { dataSource, EntityTrigger, log } from '@graphprotocol/graph-ts'
import { AggregatedData } from '../generated/schema'
import { SourceAData } from '../generated/subgraph-QmcYeEnoRzLgrwwW1StY7wTAGSS8Qt5G78RYAHHyLJWYBy'
import { SourceBData } from '../generated/subgraph-QmetPUZD9SNGjdpkgefBGe8uWEcLYxggVaixkYKd22AKr6'

export function handleSourceAData(data: EntityTrigger<SourceAData>): void {
  let aggregated = AggregatedData.load(data.data.id)
  if (!aggregated) {
    aggregated = new AggregatedData(data.data.id)
    aggregated.sourceA = data.data.data
    aggregated.first = 'sourceA'
  } else {
    aggregated.sourceA = data.data.data
  }
  aggregated.save()
}

export function handleSourceBData(data: EntityTrigger<SourceBData>): void {
  let aggregated = AggregatedData.load(data.data.id)
  if (!aggregated) {
    aggregated = new AggregatedData(data.data.id)
    aggregated.sourceB = data.data.data
    aggregated.first = 'sourceB'
  } else {
    aggregated.sourceB = data.data.data
  }
  aggregated.save()
}
