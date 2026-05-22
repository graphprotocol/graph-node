import { dataSource, Bytes } from "@graphprotocol/graph-ts";
import {
  IpfsFileCreated,
  ArweaveFileCreated,
} from "../generated/FileEvents/FileEvents";
import { IpfsFile, ArweaveFile } from "../generated/schema";

export function handleIpfsFileCreated(event: IpfsFileCreated): void {
  dataSource.create("IpfsFile", [event.params.cid]);
}

export function handleArweaveFileCreated(event: ArweaveFileCreated): void {
  dataSource.create("ArweaveFile", [event.params.txId]);
}

export function handleIpfsFile(data: Bytes): void {
  let entity = new IpfsFile(dataSource.stringParam());
  entity.content = data.toString();
  entity.save();
}

export function handleArweaveFile(data: Bytes): void {
  let entity = new ArweaveFile(dataSource.stringParam());
  entity.content = data.toString();
  entity.save();
}
