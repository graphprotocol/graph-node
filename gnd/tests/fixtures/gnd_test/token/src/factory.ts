import { FactoryTokenCreated } from '../generated/Factory/TokenFactory'
import { Token } from '../generated/schema'
import { FactoryToken } from '../generated/templates';
import { ERC20 } from '../generated/templates/FactoryToken/ERC20';

export function handleTokenCreated(event: FactoryTokenCreated): void {
  FactoryToken.create(event.params.addr);
  let token = new Token(event.params.addr);

  // Try to fetch token details using ERC20 interface
  let erc20 = ERC20.bind(event.params.addr);
  let nameResult = erc20.try_name();
  let symbolResult = erc20.try_symbol();
  let decimalsResult = erc20.try_decimals();

  // If the calls revert, we can set default values
  token.name = nameResult.reverted ? "Unknown" : nameResult.value;
  token.symbol = symbolResult.reverted ? "Unknown" : symbolResult.value;
  token.decimals = decimalsResult.reverted ? 18 : decimalsResult.value;
  token.save();
}
