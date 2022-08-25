import { ethereum, dataSource, BigInt, Bytes } from '@graphprotocol/graph-ts'
import { IpfsFile } from '../generated/schema'

export function handleBlock(block: ethereum.Block): void {
  if (block.number == BigInt.fromI32(0)) {
    // CID QmVkvoPGi9jvvuxsHDVJDgzPEzagBaWSZRYoRDzU244HjZ is the file
    // `file-data-sources/abis/Contract.abi` after being processed by graph-cli.
    dataSource.create("File", ["QmVkvoPGi9jvvuxsHDVJDgzPEzagBaWSZRYoRDzU244HjZ"])
  }
}

export function handleFile(data: Bytes): void {
  let entity = new IpfsFile(dataSource.address().toHexString())
  entity.content = data.toString()
  entity.save()
}
