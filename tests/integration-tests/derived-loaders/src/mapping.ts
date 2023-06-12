import { Trigger } from "../generated/Contract/Contract";
import { Bar, Foo } from "../generated/schema";

export function handleTrigger(event: Trigger): void {
  let obj = new Foo("0");
  obj.value = 1;
  obj.save();

  let obj2 = new Bar("1");
  obj2.fooValue = "0";
  obj2.save();

  let obj3 = Foo.load("0");
  let obj4 = obj3!.bar.load();


  assert(obj4 !== null, "obj4 should not be null");
}
