import { dataSource, EntityTrigger, log } from '@graphprotocol/graph-ts'
import { AggregatedData } from '../generated/schema'
import { SourceAData } from '../generated/subgraph-QmYHp1bPEf7EoYBpEtJUpZv1uQHYQfWE4AhvR6frjB1Huj'
import { SourceBData } from '../generated/subgraph-QmYBEzastJi7bsa722ac78tnZa6xNnV9vvweerY4kVyJtq'


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
