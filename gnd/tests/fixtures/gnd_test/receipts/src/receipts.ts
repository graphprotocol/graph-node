import { Transfer as TransferEvent } from '../generated/Receipts/ERC20'
import { Transfer, Approval } from '../generated/schema'
import { Bytes, ethereum, log } from "@graphprotocol/graph-ts";

const APPROVAL_TOPIC0 = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"

export function handleEventsWithReceipts(event: TransferEvent): void {
  let receipt = event.receipt;

  if (!receipt) {
    log.error("No receipt found for transaction {}", [event.transaction.hash.toHexString()]);
    return;
  }

  // iterate over the logs in the receipt and find the Approval event
  for (let i = 0; i < receipt.logs.length; i++) {
    let logEntry = receipt.logs[i];

    if (logEntry.topics[0].toHexString() == APPROVAL_TOPIC0) {
      let approval = new Approval(event.transaction.hash);
      // trim the padding from the topics to get the actual address
      approval.owner = changetype<Bytes>(logEntry.topics[1].subarray(12, 32));
      approval.spender = changetype<Bytes>(logEntry.topics[2].subarray(12, 32));
      let data = ethereum.decode("uint256", logEntry.data);
      if (data) {
        approval.value = data.toBigInt();
      } else {
        log.error("Failed to decode approval value for transaction {}", [event.transaction.hash.toHexString()]);
        return;
      }
      approval.save();
    }
  }

  let transfer = new Transfer(event.transaction.hash);
  transfer.from = event.params.from;
  transfer.to = event.params.to;
  transfer.value = event.params.value;
  transfer.save();
}
