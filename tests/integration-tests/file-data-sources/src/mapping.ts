import { ethereum, dataSource, BigInt, Bytes } from '@graphprotocol/graph-ts'
import { IpfsFile, IpfsFile1, Holder, Token } from '../generated/schema'

export function handleBlock(block: ethereum.Block): void {
  // This will create the same data source twice, once at block 0 and another at block 2.
  // The creation at block 2 should be detected as a duplicate and therefore a noop.
  if (block.number == BigInt.fromI32(0) || block.number == BigInt.fromI32(2)) {
    // CID QmVkvoPGi9jvvuxsHDVJDgzPEzagBaWSZRYoRDzU244HjZ is the file
    // `file-data-sources/abis/Contract.abi` after being processed by graph-cli.
    dataSource.create("File", ["QmVkvoPGi9jvvuxsHDVJDgzPEzagBaWSZRYoRDzU244HjZ"])
  }

  if (block.number == BigInt.fromI32(1)) {
    // Test that using an invalid CID will be ignored
    dataSource.create("File", ["hi, I'm not valid"])
    let holder = new Holder("1");
    holder.save();
    let token = new Token("1");
    token.holder = "1";
    token.color = "bliss";
    token.save();
  }


  // This will invoke File1 data source with same CID, which will be used 
  // to test whether same cid is triggered across different data source.
  if (block.number == BigInt.fromI32(3)) {
    dataSource.create("File1", ["QmVkvoPGi9jvvuxsHDVJDgzPEzagBaWSZRYoRDzU244HjZ"])
    let holder = Holder.load("1");
    if (holder != null) {
      let token = holder.tokens.load();
      if (token != null) {
        token.color = "joy";
        token.save();
      }
    }

  }
}

export function handleFile(data: Bytes): void {
  let entity = new IpfsFile(dataSource.stringParam())
  entity.content = data.toString()
  entity.save()
}

export function handleFile1(data: Bytes): void {
  let entity = new IpfsFile1(dataSource.stringParam())
  entity.content = data.toString()
  entity.save()
}
