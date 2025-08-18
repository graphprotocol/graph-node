import { ethereum, log, BigInt, Address } from "@graphprotocol/graph-ts";
import { AssetTransfer, ComplexAssetCreated, Contract } from "../generated/Contract/Contract";
import { AssetTransferCall, ComplexAssetCall, StructFieldTest } from "../generated/schema";

export function handleAssetTransfer(event: AssetTransfer): void {
  let id = event.transaction.hash.toHex() + "-" + event.logIndex.toString();
  let assetTransferCall = new AssetTransferCall(id);

  // Store event data
  assetTransferCall.assetAddr = event.params.asset.addr;
  assetTransferCall.assetAmount = event.params.asset.amount;
  assetTransferCall.assetActive = event.params.asset.active;
  assetTransferCall.to = event.params.to;
  assetTransferCall.blockNumber = event.block.number;
  assetTransferCall.transactionHash = event.transaction.hash;

  // Test struct field access by index; the mapping code uses named fields,
  // but the underlying calls in the manifest are declared using an index
  const contract = Contract.bind(event.address);
  let ownerCall = contract.try_getOwner(event.params.asset.addr);
  if (!ownerCall.reverted) {
    assetTransferCall.owner = ownerCall.value;
    createStructFieldTest(id + "-owner", "asset_owner", "addr", true, ownerCall.value.toString(), null, event.block.number);
  } else {
    assetTransferCall.owner = Address.zero();
    createStructFieldTest(id + "-owner", "asset_owner", "addr", false, null, "Call failed", event.block.number);
  }

  let metadataCall = contract.try_getMetadata(event.params.asset.addr);
  if (!metadataCall.reverted) {
    assetTransferCall.metadata = metadataCall.value.toString();
    createStructFieldTest(id + "-metadata-by-name", "asset_metadata", "addr", true, metadataCall.value.toString(), null, event.block.number);
  } else {
    assetTransferCall.metadata = "";
    createStructFieldTest(id + "-metadata-by-name", "asset_metadata", "addr", false, null, "Call failed", event.block.number);
  }

  let amountCalcCall = contract.try_sum(event.params.asset.amount, event.params.asset.amount);
  if (!amountCalcCall.reverted) {
    assetTransferCall.amountCalc = amountCalcCall.value;
    createStructFieldTest(id + "-amount-by-name", "asset_amount", "amount", true, amountCalcCall.value.toString(), null, event.block.number);
  } else {
    assetTransferCall.amountCalc = BigInt.fromI32(0);
    createStructFieldTest(id + "-amount-by-name", "asset_amount", "amount", false, null, "Call failed", event.block.number);
  }

  // Regular call (not using struct fields)
  let balanceCall = contract.try_balanceOf(event.params.to)
  if (!balanceCall.reverted) {
    assetTransferCall.recipientBalance = balanceCall.value;
  } else {
    assetTransferCall.recipientBalance = BigInt.fromI32(0);
  }

  assetTransferCall.save();
}

export function handleComplexAssetCreated(event: ComplexAssetCreated): void {
  let id = event.transaction.hash.toHex() + "-" + event.logIndex.toString();
  let complexAssetCall = new ComplexAssetCall(id);

  // Store event data
  complexAssetCall.complexAssetId = event.params.id;
  complexAssetCall.baseAssetAddr = event.params.complexAsset.base.addr;
  complexAssetCall.baseAssetAmount = event.params.complexAsset.base.amount;
  complexAssetCall.baseAssetActive = event.params.complexAsset.base.active;
  complexAssetCall.metadata = event.params.complexAsset.metadata;
  complexAssetCall.blockNumber = event.block.number;
  complexAssetCall.transactionHash = event.transaction.hash;

  // Test nested struct field access
  const contract = Contract.bind(event.address);
  let baseOwnerCall = contract.try_getOwner(event.params.complexAsset.base.addr);
  if (!baseOwnerCall.reverted) {
    complexAssetCall.baseAssetOwner = baseOwnerCall.value;
    createStructFieldTest(id + "-base-owner", "base_asset", "base.addr", true, baseOwnerCall.value.toString(), null, event.block.number);
  } else {
    complexAssetCall.baseAssetOwner = Address.zero();
    createStructFieldTest(id + "-base-owner", "base_asset", "base.addr", false, null, "Call failed", event.block.number);
  }

  let baseMetadataCall = contract.try_getMetadata(event.params.complexAsset.base.addr);
  if (!baseMetadataCall.reverted) {
    complexAssetCall.baseAssetMetadata = baseMetadataCall.value.toString();
    createStructFieldTest(id + "-base-metadata", "base_metadata", "base.addr", true, baseMetadataCall.value.toString(), null, event.block.number);
  } else {
    complexAssetCall.baseAssetMetadata = "";
    createStructFieldTest(id + "-base-metadata", "base_metadata", "base.addr", false, null, "Call failed", event.block.number);
  }

  let baseAmountCalcCall = contract.try_sum(event.params.complexAsset.base.amount, event.params.id);
  if (!baseAmountCalcCall.reverted) {
    complexAssetCall.baseAssetAmountCalc = baseAmountCalcCall.value;
    createStructFieldTest(id + "-base-amount", "base_amount", "base.amount", true, baseAmountCalcCall.value.toString(), null, event.block.number);
  } else {
    complexAssetCall.baseAssetAmountCalc = BigInt.fromI32(0);
    createStructFieldTest(id + "-base-amount", "base_amount", "base.amount", false, null, "Call failed", event.block.number);
  }

  complexAssetCall.save();
}

function createStructFieldTest(
  id: string,
  testType: string,
  fieldName: string,
  success: boolean,
  result: string | null,
  error: string | null,
  blockNumber: BigInt
): void {
  let test = new StructFieldTest(id);
  test.testType = testType;
  test.fieldName = fieldName;
  test.success = success;
  test.result = result;
  test.error = error;
  test.blockNumber = blockNumber;
  test.save();
}
