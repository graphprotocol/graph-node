import { ethereum, log, BigInt } from "@graphprotocol/graph-ts";
import { Contract, Transfer } from "../generated/Contract/Contract";
import { TransferCall, CallResult } from "../generated/schema";

export function handleTransfer(event: Transfer): void {
  let id = event.transaction.hash.toHex() + "-" + event.logIndex.toString();
  let transferCall = new TransferCall(id);

  transferCall.from = event.params.from;
  transferCall.to = event.params.to;
  transferCall.value = event.params.value;
  transferCall.blockNumber = event.block.number;
  transferCall.transactionHash = event.transaction.hash;

  // Test declared calls - these should be available before the handler runs

  // Basic successful calls
  const contract = Contract.bind(event.address);
  let balanceFromCall = contract.try_balanceOf(event.params.from);
  if (!balanceFromCall.reverted) {
    transferCall.balanceFromBefore = balanceFromCall.value;
    createCallResult(id + "-balance_from", "balance_from", true, balanceFromCall.value.toString(), null);
  } else {
    transferCall.balanceFromBefore = BigInt.fromI32(0);
    createCallResult(id + "-balance_from", "balance_from", false, null, "Call failed");
  }

  let balanceToCall = contract.try_balanceOf(event.params.to);
  if (!balanceToCall.reverted) {
    transferCall.balanceToBefore = balanceToCall.value;
    createCallResult(id + "-balance_to", "balance_to", true, balanceToCall.value.toString(), null);
  } else {
    transferCall.balanceToBefore = BigInt.fromI32(0);
    createCallResult(id + "-balance_to", "balance_to", false, null, "Call failed");
  }

  let totalSupplyCall = contract.try_totalSupply();
  if (!totalSupplyCall.reverted) {
    transferCall.totalSupply = totalSupplyCall.value;
    createCallResult(id + "-total_supply", "total_supply", true, totalSupplyCall.value.toString(), null);
  } else {
    transferCall.totalSupply = BigInt.fromI32(0);
    createCallResult(id + "-total_supply", "total_supply", false, null, "Call failed");
  }

  let constantCall = contract.try_getConstant();
  if (!constantCall.reverted) {
    transferCall.constantValue = constantCall.value;
    createCallResult(id + "-constant_value", "constant_value", true, constantCall.value.toString(), null);
  } else {
    transferCall.constantValue = BigInt.fromI32(0);
    createCallResult(id + "-constant_value", "constant_value", false, null, "Call failed");
  }

  let sumCall = contract.try_sum(event.params.value, event.params.value);
  if (!sumCall.reverted) {
    transferCall.sumResult = sumCall.value;
    createCallResult(id + "-sum_values", "sum_values", true, sumCall.value.toString(), null);
  } else {
    transferCall.sumResult = BigInt.fromI32(0);
    createCallResult(id + "-sum_values", "sum_values", false, null, "Call failed");
  }

  let metadataCall = contract.try_getMetadata(event.params.from);
  if (!metadataCall.reverted) {
    transferCall.metadataFrom = metadataCall.value.toString();
    createCallResult(id + "-metadata_from", "metadata_from", true, metadataCall.value.toString(), null);
  } else {
    transferCall.metadataFrom = "";
    createCallResult(id + "-metadata_from", "metadata_from", false, null, "Call failed");
  }

  // Test call that should revert
  let revertCall = contract.try_alwaysReverts();
  transferCall.revertCallSucceeded = !revertCall.reverted;
  if (!revertCall.reverted) {
    createCallResult(id + "-will_revert", "will_revert", true, revertCall.value.toString(), null);
    log.warning("Expected revert call succeeded unexpectedly", []);
  } else {
    createCallResult(id + "-will_revert", "will_revert", false, null, "Call reverted as expected");
    log.info("Revert call failed as expected", []);
  }

  transferCall.save();
}

function createCallResult(id: string, label: string, success: boolean, value: string | null, error: string | null): void {
  let callResult = new CallResult(id);
  callResult.label = label;
  callResult.success = success;
  callResult.value = value;
  callResult.error = error;
  callResult.save();
}
