use wasmi::{RuntimeArgs, RuntimeValue, Trap};

use graph::prelude::BigInt;

use crate::asc_abi::asc_ptr::*;
use crate::asc_abi::class::*;
use crate::asc_abi::*;
use crate::module::WasmiModule;
use crate::{HostFunction, HostModule};

pub struct CoreModule {
  functions: Vec<HostFunction>,
}

impl CoreModule {
  pub fn new() -> Self {
    Self {
      functions: vec![
        HostFunction {
          name: "gas".into(),
          full_name: "gas".into(),
          metrics_name: "gas".into(),
        },
        HostFunction {
          name: "abort".into(),
          full_name: "abort".into(),
          metrics_name: "abort".into(),
        },
        HostFunction {
          name: "bigIntToHex".into(),
          full_name: "typeConversion.bigIntToHex".into(),
          metrics_name: "typeConversion_bigIntToHex".into(),
        },
        HostFunction {
          name: "get".into(),
          full_name: "store.get".into(),
          metrics_name: "store_get".into(),
        },
        HostFunction {
          name: "set".into(),
          full_name: "store.set".into(),
          metrics_name: "store_set".into(),
        },
      ],
    }
  }

  /// function typeConversion.bigIntToHex(n: Uint8Array): string
  fn big_int_to_hex(
    &self,
    heap: &mut WasmiModule,
    big_int_ptr: AscPtr<AscBigInt>,
  ) -> Result<Option<RuntimeValue>, Trap> {
    let n: BigInt = heap.asc_get(big_int_ptr);
    let s = if n == 0.into() {
      "0x0".to_string()
    } else {
      let bytes = n.to_bytes_be().1;
      format!("0x{}", ::hex::encode(bytes).trim_start_matches('0'))
    };
    Ok(Some(RuntimeValue::from(heap.asc_new(&s))))
  }
}

impl HostModule for CoreModule {
  fn name(&self) -> &str {
    ""
  }
  fn functions(&self) -> &Vec<HostFunction> {
    &self.functions
  }

  fn invoke(
    &self,
    heap: &mut WasmiModule,
    full_name: &str,
    args: RuntimeArgs<'_>,
  ) -> Result<Option<RuntimeValue>, Trap> {
    match dbg!(full_name) {
      "gas" => Ok(None),
      "abort" => Ok(None),
      "typeConversion.bigIntToHex" => self.big_int_to_hex(heap, args.nth_checked(0)?),
      "store.get" => Ok(None),
      "store.set" => Ok(None),
      _ => todo!(),
    }
  }
}

pub struct EnvModule {}

pub struct StoreModule {}

pub struct IpfsModule {}
