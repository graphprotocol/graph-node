import { Trigger } from "../generated/Contract/Contract";
import { BigDecimal, BigInt } from "@graphprotocol/graph-ts";

// Test that host exports work in globals.
let one = BigDecimal.fromString("1");

export function handleTrigger(event: Trigger): void {
  // There are 35 digits after the dot.
  // big_decimal exponent will be: -35 - 6109 = -6144.
  // Minmum exponent is: -6143.
  // So 1 digit will be removed, the 8, and the 6 will be rounded to 7.
  let small = BigDecimal.fromString("0.99999999999999999999999999999999968");

  small.exp -= BigInt.fromI32(6109);

  // Round-trip through the node so it truncates.
  small = small * new BigDecimal(BigInt.fromI32(1));

  assert(small.exp == BigInt.fromI32(-6143), "wrong exp");

  // This has 33 nines and the 7 which was rounded from 6.
  assert(
    small.digits ==
      BigDecimal.fromString("9999999999999999999999999999999997").digits,
    "wrong digits " + small.digits.toString()
  );

  // This has 35 nines, but we round to 34 decimal digits.
  let big = BigDecimal.fromString("99999999999999999999999999999999999");

  // This has 35 zeros.
  let rounded = BigDecimal.fromString("100000000000000000000000000000000000");

  assert(big == rounded, "big not equal to rounded");

  // This has 35 eights, but we round to 34 decimal digits.
  let big2 = BigDecimal.fromString("88888888888888888888888888888888888");

  // This has 33 eights.
  let rounded2 = BigDecimal.fromString("88888888888888888888888888888888890");

  assert(big2 == rounded2, "big2 not equal to rounded2 " + big2.toString());

  // Test big decimal division.
  assert(one / BigDecimal.fromString("10") == BigDecimal.fromString("0.1"));
}
