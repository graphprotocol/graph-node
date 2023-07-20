import { Bytes, store } from "@graphprotocol/graph-ts";
import { Trigger } from "../generated/Contract/Contract";
import { Bar, Foo, BFoo, BBar } from "../generated/schema";

export function handleTrigger(event: Trigger): void {
  let foo = new Foo("0");
  foo.value = 1;
  foo.save();

  let bar = new Bar("1");
  bar.fooValue = "0";
  bar.save();

  let fooLoaded = Foo.load("0");

  let barDerived = fooLoaded!.bar.load();

  assert(barDerived !== null, "barDerived should not be null");

  let bFoo = new BFoo(Bytes.fromUTF8("0"));
  bFoo.value = 1;
  bFoo.save();

  let bBar = new BBar(Bytes.fromUTF8("1"));
  bBar.fooValue = Bytes.fromUTF8("0");
  bBar.save();

  let bFooLoaded = BFoo.load(Bytes.fromUTF8("0"));
  let bBarDerived = changetype<Bar[]>(
    store.loadRelated("BFoo", bFooLoaded!.id.toHexString(), "bar")
  );

  assert(bBarDerived !== null, "bBarDerived should not be null");
}
