import { dataSource, EntityTrigger, log } from '@graphprotocol/graph-ts'
import { AggregatedData } from '../generated/schema'
import { SourceAData } from '../generated/subgraph-QmZBecjQfrQG5BfpapLywSAzVb5FSFty4j9hVSAhkxbBas'
import { SourceBData } from '../generated/subgraph-QmaqX7yefmvgVTbc2ZukVYasSgXtE7Xg5b79Z7afVx4y6u'


// We know this handler will run first since its defined first in the manifest
// So we dont need to check if the Aggregated data exists
export function handleSourceAData(data: SourceAData): void {
  let aggregated = new AggregatedData(data.id)
  aggregated.sourceA = data.data
  aggregated.first = 'sourceA'
  aggregated.save()
}

export function handleSourceBData(data: SourceBData): void {
  let aggregated = AggregatedData.load(data.id)
  if (!aggregated) {
    aggregated = new AggregatedData(data.id)
    aggregated.sourceB = data.data
    aggregated.first = 'sourceB'
  } else {
    aggregated.sourceB = data.data
  }
  aggregated.save()
}
