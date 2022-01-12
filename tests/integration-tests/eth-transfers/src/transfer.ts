import { Transfer, TransferCall } from "../generated/transfer/transfer"
import { Block, Payment } from "../generated/schema"
import { ethereum } from '@graphprotocol/graph-ts'

export function handleTransferEvent(event: Transfer): void {
  let payment = new Payment(event.block.number.toString())
  payment.from = event.transaction.from.toHex()
  payment.to = event.transaction.to!.toHex()
  payment.value = event.transaction.value

  payment.save()
}

export function handleTransferCall(call: TransferCall): void {
  let payment = new Payment(call.block.number.toString())
  payment.from = call.from.toHex()
  payment.to = call.inputs._to.toHex()
  payment.value = call.inputs._value
  payment.save()
}

export function handleBlock(block: ethereum.Block): void {
  let id = block.hash.toHex()

  let entity = new Block(id)
  entity.number = block.number
  entity.save()
}

