import { Bytes, BigInt, log, store } from "@graphprotocol/graph-ts";
import { TestEvent } from "../generated/Contract/Contract";
import { Bar, Foo, BFoo, BBar, TestResult } from "../generated/schema";

export function handleTestEvent(event: TestEvent): void {
  if (event.params.testCommand == "0") {
    let foo = new Foo("0");
    foo.value = 1;
    foo.save();

    let bar = new Bar("0");
    bar.fooValue = "0";
    bar.save();

    let bFoo = new BFoo(Bytes.fromUTF8("0"));
    bFoo.value = 1;
    bFoo.save();

    let bBar = new BBar(Bytes.fromUTF8("0"));
    bBar.fooValue = Bytes.fromUTF8("0");
    bBar.save();

    let fooLoaded = Foo.load("0");
    let barDerived = fooLoaded!.bar.load();
    let bFooLoaded = BFoo.load(Bytes.fromUTF8("0"));
    let bBarDerived: BBar[] = bFooLoaded!.bar.load();

    let testResult = new TestResult("0");
    if (barDerived.length > 0) {
      testResult.barDerived = barDerived[0].id;
    }

    if (bBarDerived.length > 0) {
      testResult.bBarDerived = bBarDerived[0].id;
    }

    testResult.save();
  }

  if (event.params.testCommand == "1") {
    let fooLoaded = Foo.load("0");
    let barDerived = fooLoaded!.bar.load();
    let bFooLoaded = BFoo.load(Bytes.fromUTF8("0"));
    let bBarDerived = bFooLoaded!.bar.load();

    let testResult1 = new TestResult("1");

    if (barDerived.length > 0) {
      testResult1.barDerived = barDerived[0].id;
    }

    if (bBarDerived.length > 0) {
      testResult1.bBarDerived = bBarDerived[0].id;
    }
    testResult1.save();
  }
}