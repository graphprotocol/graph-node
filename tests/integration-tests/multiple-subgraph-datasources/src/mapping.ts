import { dataSource, EntityTrigger, log } from '@graphprotocol/graph-ts'
import { AggregatedData } from '../generated/schema'
import { SourceAData } from '../generated/subgraph-QmPWnNsD4m8T9EEF1ec5d8wetFxrMebggLj1efFHzdnZhx'
import { SourceBData } from '../generated/subgraph-Qma4Rk2D1w6mFiP15ZtHHx7eWkqFR426RWswreLiDanxej'

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
