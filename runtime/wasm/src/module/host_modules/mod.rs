mod type_conversion;
pub use self::type_conversion::TypeConversionModule;

mod store;
pub use self::store::StoreModule;

#[macro_export]
macro_rules! host_module {
  ($module:ty, $funcs:ident, $ns:ident, {$($efname:ident => $ifname:ident [$($x:literal),*],)*}) => {
      ::lazy_static::lazy_static! {
          pub static ref $funcs: Vec<$crate::HostFunction> = vec![
              $(
                  $crate::HostFunction::from(
                      format!(
                          "{}.{}",
                          stringify!($ns),
                          stringify!($efname)
                      )
                  ),
              )*
          ];
      }

      impl $crate::HostModule for $module {
          fn name(&self) -> &str {
              stringify!(ns)
          }

          fn functions(&self) -> &Vec<$crate::HostFunction> {
              &$funcs
          }

          fn invoke(
              &self,
              module: &mut $crate::WasmiModule,
              full_name: &str,
              args: $crate::wasmi::RuntimeArgs,
          ) -> Result<Option<$crate::wasmi::RuntimeValue>, $crate::HostModuleError> {
              match full_name {
                  $(
                      concat!(stringify!($ns), ".", stringify!($efname)) => self.$ifname(module, $(args.nth_checked($x)?,)*),
                  )*
                  _ => unimplemented!(),
              }
          }
      }
  };
}
