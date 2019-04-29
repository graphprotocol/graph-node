import { Address, DataSourceTemplate } from "@graphprotocol/graph-ts";

export class Exchange extends DataSourceTemplate {
  static create(address: Address): void {
    DataSourceTemplate.create("Exchange", [address.toHex()]);
  }
}
