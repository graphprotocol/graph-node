import { Trigger } from "../generated/Contract/Contract";
import { BigDecimal, BigInt } from "@graphprotocol/graph-ts";

export function handleTrigger(event: Trigger): void {
  // There are 35 digits after the dot.
  // big_decimal exponent will be: -35 - 6109 = -6144.
  // Minmum exponent is: -6143.
  // So 1 digit will be removed, the 8, and the 6 will be rounded to 7.
  let big_decimal = BigDecimal.fromString(
    "0.99999999999999999999999999999999968"
  );

  big_decimal.exp -= BigInt.fromI32(6109);

  // Round-trip through the node so it truncates.
  big_decimal = big_decimal * new BigDecimal(BigInt.fromI32(1));

  assert(big_decimal.exp == BigInt.fromI32(-6143), "wrong exp");

  // This has 33 nines and the 7 which was rounded from 6.
  assert(
    big_decimal.digits ==
      BigDecimal.fromString("9999999999999999999999999999999997").digits,
    "fug " + big_decimal.digits.toString()
  );
}
