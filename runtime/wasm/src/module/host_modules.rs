use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use wasmi::{RuntimeArgs, RuntimeValue};

use graph::prelude::{
    debug, format_err, Attribute, BigInt, BlockState, Entity, EntityKey, Logger,
    SubgraphDeploymentId, Value,
};
use graph_graphql::schema::ast::validate_entity;

use crate::asc_abi::asc_ptr::*;
use crate::asc_abi::class::*;
use crate::asc_abi::*;
use crate::module::WasmiModule;
use crate::{HostFunction, HostModule, HostModuleError, RuntimeStore};

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
            ],
        }
    }

    /// function typeConversion.bigIntToHex(n: Uint8Array): string
    fn big_int_to_hex(
        &self,
        module: &mut WasmiModule,
        big_int_ptr: AscPtr<AscBigInt>,
    ) -> Result<Option<RuntimeValue>, HostModuleError> {
        let n: BigInt = module.asc_get(big_int_ptr);
        let s = if n == 0.into() {
            "0x0".to_string()
        } else {
            let bytes = n.to_bytes_be().1;
            format!("0x{}", ::hex::encode(bytes).trim_start_matches('0'))
        };
        Ok(Some(RuntimeValue::from(module.asc_new(&s))))
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
        module: &mut WasmiModule,
        full_name: &str,
        args: RuntimeArgs<'_>,
    ) -> Result<Option<RuntimeValue>, HostModuleError> {
        match dbg!(full_name) {
            "gas" => Ok(None),
            "abort" => Ok(None),
            "typeConversion.bigIntToHex" => self.big_int_to_hex(module, args.nth_checked(0)?),
            _ => todo!(),
        }
    }
}

pub struct EnvModule {}

pub struct StoreModule {
    functions: Vec<HostFunction>,
}

impl StoreModule {
    pub fn new() -> Self {
        Self {
            functions: vec![
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

    pub(crate) fn store_set(
        &self,
        module: &mut WasmiModule,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
        data_ptr: AscPtr<AscEntity>,
    ) -> Result<Option<RuntimeValue>, HostModuleError> {
        let entity_type: String = module.asc_get(entity_ptr);
        let entity_id: String = module.asc_get(id_ptr);
        let mut data: HashMap<Attribute, Value> = module.asc_get(data_ptr);

        // Automatically add an "id" value
        match data.insert("id".to_string(), Value::String(entity_id.clone())) {
            Some(ref v) if v != &Value::String(entity_id.clone()) => {
                return Err(HostModuleError(format_err!(
                    "Value of {} attribute 'id' conflicts with ID passed to `store.set()`: \
                 {} != {}",
                    entity_type,
                    v,
                    entity_id,
                )));
            }
            _ => (),
        }

        let key = EntityKey {
            subgraph_id: module.ctx.subgraph_id.clone(),
            entity_type,
            entity_id,
        };
        let entity = Entity::from(data);
        let schema = module.ctx.store.input_schema(&module.ctx.subgraph_id)?;
        let is_valid = validate_entity(&schema.document, &key, &entity).is_ok();
        module.ctx.state.entity_cache.set(key.clone(), entity);

        // Validate the changes against the subgraph schema.
        // If the set of fields we have is already valid, avoid hitting the DB.
        if !is_valid
            && module
                .ctx
                .store
                .uses_relational_schema(&module.ctx.subgraph_id)?
        {
            let entity = module
                .ctx
                .state
                .entity_cache
                .get(module.ctx.store.as_ref(), &key)?
                .expect("we just stored this entity");
            validate_entity(&schema.document, &key, &entity)?;
        }

        Ok(None)
    }

    pub fn store_remove(&self, module: &mut WasmiModule, entity_type: String, entity_id: String) {
        let key = EntityKey {
            subgraph_id: module.ctx.subgraph_id.clone(),
            entity_type,
            entity_id,
        };
        module.ctx.state.entity_cache.remove(key);
    }

    pub(crate) fn store_get(
        &self,
        module: &mut WasmiModule,
        entity_type_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
    ) -> Result<Option<RuntimeValue>, HostModuleError> {
        let entity_type: String = module.asc_get(entity_type_ptr);
        let entity_id: String = module.asc_get(id_ptr);

        let start_time = Instant::now();
        let store_key = EntityKey {
            subgraph_id: module.ctx.subgraph_id.clone(),
            entity_type: entity_type.clone(),
            entity_id: entity_id.clone(),
        };

        let result = module
            .ctx
            .state
            .entity_cache
            .get(module.ctx.store.as_ref(), &store_key)
            .map_err(|e| HostModuleError(e.into()))
            .map(|ok| ok.to_owned());

        debug!(
          module.ctx.logger,
           "Store get finished";
           "type" => &entity_type,
           "id" => &entity_id,
           "time" => format!("{}ms", start_time.elapsed().as_millis())
        );

        result
            .map(|entity_opt| entity_opt.map(|entity| RuntimeValue::from(module.asc_new(&entity))))
    }
}

impl HostModule for StoreModule {
    fn name(&self) -> &str {
        "store"
    }

    fn functions(&self) -> &Vec<HostFunction> {
        &self.functions
    }

    fn invoke(
        &self,
        module: &mut WasmiModule,
        full_name: &str,
        args: RuntimeArgs<'_>,
    ) -> Result<Option<RuntimeValue>, HostModuleError> {
        match full_name {
            "store.get" => self.store_get(module, args.nth_checked(0)?, args.nth_checked(1)?),
            "store.set" => self.store_set(
                module,
                args.nth_checked(0)?,
                args.nth_checked(1)?,
                args.nth_checked(2)?,
            ),
            _ => todo!(),
        }
    }
}

pub struct IpfsModule {}
