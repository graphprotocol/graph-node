import { Transfer } from '../generated/StandardToken/ERC20'
import { Token, Account, Balance } from '../generated/schema'
import { ERC20 } from '../generated/StandardToken/ERC20';
import { log } from "@graphprotocol/graph-ts";

export function handleTransfer(event: Transfer): void {
  let token = Token.load(event.address);
  let from = Account.load(event.params.from);
  let to = Account.load(event.params.to);
  let balanceFrom = Balance.load(event.params.from.concat(event.address));
  let balanceTo = Balance.load(event.params.to.concat(event.address));

  let erc20 = ERC20.bind(event.address);

  if (!token) {
    token = new Token(event.address);

    // Try to fetch token details using ERC20 interface
    let nameResult = erc20.try_name();
    let symbolResult = erc20.try_symbol();
    let decimalsResult = erc20.try_decimals();

    // If the calls revert, we can set default values
    token.name = nameResult.reverted ? "Unknown" : nameResult.value;
    token.symbol = symbolResult.reverted ? "Unknown" : symbolResult.value;
    token.decimals = decimalsResult.reverted ? 18 : decimalsResult.value;
    token.save();
  }

  if (!from) {
    from = new Account(event.params.from);
    from.save();
  }

  if (!to) {
    to = new Account(event.params.to);
    to.save();
  }

  if (!balanceFrom) {
    log.warning("Balance for account {} and token {} does not exist. Creating new balance entity.", [event.params.from.toHexString(), event.address.toHexString()]);
    balanceFrom = new Balance(event.params.from.concat(event.address));
    balanceFrom.account = event.params.from;
    balanceFrom.token = event.address;
    let balanceFromResult = erc20.try_balanceOf(event.params.from);
    balanceFrom.amount = balanceFromResult.reverted ? null : balanceFromResult.value;
  } else {
    log.warning("Balance for account {} and token {} already exists. Updating balance based on transfer value.", [event.params.from.toHexString(), event.address.toHexString()]);
    // If the balance already exists, we need to update it based on the transfer value
    // We will fetch the current balance and then update it after the transfer
    let currentBalance = balanceFrom.amount;

    if (currentBalance) {
      let newBalance = currentBalance.minus(event.params.value);
      balanceFrom.amount = newBalance;
    } else {
      // Try to fetch the balance if it was not previously set
      let balanceFromResult = erc20.try_balanceOf(event.params.from);
      balanceFrom.amount = balanceFromResult.reverted ? null : balanceFromResult.value;
    }
  }

  if (!balanceTo) {
    balanceTo = new Balance(event.params.to.concat(event.address));
    balanceTo.account = event.params.to;
    balanceTo.token = event.address;
    let balanceToResult = erc20.try_balanceOf(event.params.to);
    balanceTo.amount = balanceToResult.reverted ? null : balanceToResult.value;
  } else {
    // If the balance already exists, we need to update it based on the transfer value
    // We will fetch the current balance and then update it after the transfer
    let currentBalance = balanceTo.amount;

    if (currentBalance) {
      let newBalance = currentBalance.plus(event.params.value);
      balanceTo.amount = newBalance;
    } else {
      // Try to fetch the balance if it was not previously set
      let balanceToResult = erc20.try_balanceOf(event.params.to);
      balanceTo.amount = balanceToResult.reverted ? null : balanceToResult.value;
    }
  }

  balanceFrom.save();
  balanceTo.save();
}
