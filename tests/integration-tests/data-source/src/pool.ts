import { ethereum } from "@graphprotocol/graph-ts"
import { Factory, Pool, Swap, Transaction } from "../generated/schema"
import { FACTORY_ADDRESS, ONE_BI } from "./utils"
import { Swap as SwapEvent } from "../generated/templates/Pool/Pool"

export function handleSwap(event: SwapEvent): void {
  let factory = Factory.load(FACTORY_ADDRESS)
  if (factory == null) {
    throw "Factory should exist"
  }
  factory.txCount = factory.txCount.plus(ONE_BI)
  factory.save()

  let pool = Pool.load(event.address.toHexString())
  if (pool == null) {
    throw "Pool should exist"
  }
  pool.txCount = pool.txCount.plus(ONE_BI)
  pool.save()

  let transaction = loadTransaction(event)
  let swap = new Swap(transaction.id + "#" + pool.txCount.toString())
  swap.transaction = transaction.id
  swap.timestamp = transaction.timestamp
  swap.pool = pool.id
  swap.sender = event.params.sender
  swap.origin = event.transaction.from
  swap.recipient = event.params.recipient
  swap.logIndex = event.logIndex
  swap.save()
}

export function loadTransaction(event: ethereum.Event): Transaction {
  let transaction = Transaction.load(event.transaction.hash.toHexString())
  if (transaction === null) {
    transaction = new Transaction(event.transaction.hash.toHexString())
  }
  transaction.blockNumber = event.block.number
  transaction.timestamp = event.block.timestamp
  transaction.save()

  return transaction as Transaction
}
