import { Address, log } from '@graphprotocol/graph-ts'
import { Trigger } from "../generated/Contract/Contract"

export function handleTrigger(_: Trigger): void {
  log.info("ADDRESS CORRUPTION TEST - ADDRESS: {}", ["0x3b44b2a187a7b3824131f8db5a74194d0a42fc15"])
  log.info("ADDRESS CORRUPTION TEST - LENGTH: {}", ["0x3b44b2a187a7b3824131f8db5a74194d0a42fc15".length.toString()])

  // This call should fail with: "Failed to convert string to Address/H160: '': Invalid input length"
  let address = Address.fromString("0x3b44b2a187a7b3824131f8db5a74194d0a42fc15")

  // If we reach here, the bug is not reproduced
  log.info("ADDRESS CORRUPTION TEST - ALL GOOD: {}", [address.toHexString()])
}
