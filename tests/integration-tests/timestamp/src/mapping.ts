import { Trigger } from "../generated/Contract/Contract";
import { Foo } from "../generated/schema";

export function handleTrigger(event: Trigger): void {
  let obj = new Foo("0");
  obj.value = 1710837304040956;
  obj.save();

  obj = <Foo>Foo.load("0");
  assert(obj.value == 1710837304040956, "maybe invalid value");
}
