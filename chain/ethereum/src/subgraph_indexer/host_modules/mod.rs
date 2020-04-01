use wasmi::{RuntimeArgs, RuntimeValue, Trap};

use graph::prelude::{HostFunction, HostModule};

pub struct Ethereum {
  functions: Vec<HostFunction>,
}

impl Ethereum {
  pub fn new() -> Self {
    Self {
      functions: vec![HostFunction {
        name: "call".into(),
        full_name: "ethereum.call".into(),
        metrics_name: "ethereum_call".into(),
      }],
    }
  }

  //  ETHEREUM_CALL_FUNC_INDEX => {
  //    let _section = stopwatch.start_section("host_export_ethereum_call");
  //
  //    // For apiVersion >= 0.0.4 the call passed from the mapping includes the
  //    // function signature; subgraphs using an apiVersion < 0.0.4 don't pass
  //    // the the signature along with the call.
  //    let arg = if self.ctx.host_exports.api_version >= Version::new(0, 0, 4) {
  //        self.asc_get::<_, AscUnresolvedContractCall_0_0_4>(args.nth_checked(0)?)
  //    } else {
  //        self.asc_get::<_, AscUnresolvedContractCall>(args.nth_checked(0)?)
  //    };
  //
  //    self.ethereum_call(arg)
  //}

  //  /// function ethereum.call(call: SmartContractCall): Array<Token> | null
  //  fn call(&mut self, call: UnresolvedContractCall) -> Result<Option<RuntimeValue>, Trap> {
  //    // let result = self
  //    //   .ctx
  //    //   .host_exports
  //    //   .ethereum_call(&self.ctx.logger, &self.ctx.block, call)?;
  //    // Ok(Some(match result {
  //    //   Some(tokens) => RuntimeValue::from(self.asc_new(tokens.as_slice())),
  //    //   None => RuntimeValue::from(0),
  //    // }))
  //
  //  }
}

impl HostModule for Ethereum {
  fn name(&self) -> &str {
    "ethereum"
  }

  fn functions(&self) -> &Vec<HostFunction> {
    &self.functions
  }

  fn invoke(&self, name: &str, args: RuntimeArgs) -> Result<Option<RuntimeValue>, Trap> {
    todo!();
  }
}
