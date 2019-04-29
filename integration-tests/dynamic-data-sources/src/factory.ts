import { NewExchange as NewExchangeEvent } from "../generated/Factory/Factory";
import { NewExchange } from "../generated/schema";
import { Exchange } from "../generated/Factory/templates";

export function handleNewExchange(event: NewExchangeEvent): void {
  let newExchange = new NewExchange(event.params.exchange.toHex());
  newExchange.name = event.params.name;
  newExchange.exchange = event.params.exchange;
  newExchange.save();

  Exchange.create(event.params.exchange);
}
