import { dataSource } from "@graphprotocol/graph-ts";
import { ethereum } from "@graphprotocol/graph-ts/chain/ethereum";

export function handleBlock(block: ethereum.Block): void {
  assert(dataSource.context().getI32("answer") == 42, "Test failed.");
}
