use std::collections::HashMap;
use std::time::Instant;

use wasmi::{RuntimeArgs, RuntimeValue};

use graph::prelude::{debug, format_err, Attribute, Entity, EntityKey, Value};
use graph_graphql::prelude::validate_entity;

use crate::asc_abi::class::{AscEntity, AscString};
use crate::asc_abi::{AscHeap, AscPtr};
use crate::module::WasmiModule;
use crate::{host_module, HostFunction, HostModule, HostModuleError};

pub struct StoreModule {}

impl StoreModule {
    pub fn new() -> Self {
        Self {}
    }
}

host_module! {
    StoreModule,
    STORE_MODULE_FUNCS,
    store,
    {
        get => get [0, 1],
        set => set [0, 1, 2],
        remove => remove [0, 1],
    }
}

impl StoreModule {
    fn get(
        &self,
        module: &mut WasmiModule,
        entity_type_ptr: AscPtr<AscString>,
        entity_id_ptr: AscPtr<AscString>,
    ) -> Result<Option<RuntimeValue>, HostModuleError> {
        let entity_type: String = module.asc_get(entity_type_ptr);
        let entity_id: String = module.asc_get(entity_id_ptr);

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

    fn set(
        &self,
        module: &mut WasmiModule,
        entity_type_ptr: AscPtr<AscString>,
        entity_id_ptr: AscPtr<AscString>,
        data_ptr: AscPtr<AscEntity>,
    ) -> Result<Option<RuntimeValue>, HostModuleError> {
        let entity_type: String = module.asc_get(entity_type_ptr);
        let entity_id: String = module.asc_get(entity_id_ptr);
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

    fn remove(
        &self,
        module: &mut WasmiModule,
        entity_type_ptr: AscPtr<AscString>,
        entity_id_ptr: AscPtr<AscString>,
    ) -> Result<Option<RuntimeValue>, HostModuleError> {
        let entity_type: String = module.asc_get(entity_type_ptr);
        let entity_id: String = module.asc_get(entity_id_ptr);

        let key = EntityKey {
            subgraph_id: module.ctx.subgraph_id.clone(),
            entity_type,
            entity_id,
        };
        module.ctx.state.entity_cache.remove(key);
        Ok(None)
    }
}
