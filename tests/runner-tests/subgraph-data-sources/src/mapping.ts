import { BigInt, Bytes, dataSource, ethereum, log } from "@graphprotocol/graph-ts";

export function handleBlock(content: Bytes): void {
  let stringContent = content.toString();
  log.info("%%%%%%% Block content: {}", [stringContent]);
}
