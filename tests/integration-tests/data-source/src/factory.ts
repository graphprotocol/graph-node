import { FACTORY_ADDRESS, ONE_BI, ZERO_BI } from "./utils"
import { Factory, Pool } from "../generated/schema"
import { PoolCreated } from "../generated/factory/factory"
import { pool as PoolTemplate } from "../generated/templates"

export function handlePoolCreated(event: PoolCreated): void {
  let factory = Factory.load(FACTORY_ADDRESS)
  if (factory == null) {
    factory = new Factory(FACTORY_ADDRESS)
    factory.poolCount = ZERO_BI
    factory.txCount = ZERO_BI
  }

  factory.poolCount = factory.poolCount.plus(ONE_BI)
  factory.save()

  let pool = new Pool(event.params.pool.toHexString()) as Pool
  pool.createdAtTimestamp = event.block.timestamp
  pool.createdAtBlockNumber = event.block.number
  pool.txCount = ZERO_BI
  pool.save()

  PoolTemplate.create(event.params.pool)
}
