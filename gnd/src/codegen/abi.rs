//! ABI code generation for Ethereum contracts.
//!
//! Generates AssemblyScript bindings from contract ABIs:
//! - Event classes with typed parameters
//! - Call classes for function calls with inputs/outputs
//! - Contract class with typed call methods

use std::collections::HashMap;

use ethabi::{Contract, Event, EventParam, Function, Param, ParamType, StateMutability};
use regex::Regex;

use super::typescript::{self as ts, Class, ClassMember, Method, ModuleImports, Param as TsParam};

/// Reserved words in AssemblyScript that need to be escaped.
const RESERVED_WORDS: &[&str] = &[
    "await",
    "break",
    "case",
    "catch",
    "class",
    "const",
    "continue",
    "debugger",
    "delete",
    "do",
    "else",
    "enum",
    "export",
    "extends",
    "false",
    "finally",
    "function",
    "if",
    "implements",
    "import",
    "in",
    "interface",
    "let",
    "new",
    "package",
    "private",
    "protected",
    "public",
    "return",
    "super",
    "switch",
    "static",
    "this",
    "throw",
    "true",
    "try",
    "typeof",
    "var",
    "while",
    "with",
    "yield",
];

/// Handle reserved words by appending an underscore.
fn handle_reserved_word(name: &str) -> String {
    if RESERVED_WORDS.contains(&name) {
        format!("{}_", name)
    } else {
        name.to_string()
    }
}

/// Capitalize the first letter of a string.
fn capitalize(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(c) => c.to_uppercase().collect::<String>() + chars.as_str(),
    }
}

const GRAPH_TS_MODULE: &str = "@graphprotocol/graph-ts";

/// ABI code generator.
pub struct AbiCodeGenerator {
    contract: Contract,
    name: String,
}

impl AbiCodeGenerator {
    /// Create a new ABI code generator.
    pub fn new(contract: Contract, name: impl Into<String>) -> Self {
        let mut name = name.into();
        // Sanitize name to be a valid class name
        let re = Regex::new(r#"[!@#$%^&*()+\-=\[\]{};':\"|,.<>/?]+"#).unwrap();
        name = re.replace_all(&name, "_").to_string();
        Self { contract, name }
    }

    /// Generate module imports for the ABI file.
    pub fn generate_module_imports(&self) -> Vec<ModuleImports> {
        vec![ModuleImports::new(
            vec![
                "ethereum".to_string(),
                "JSONValue".to_string(),
                "TypedMap".to_string(),
                "Entity".to_string(),
                "Bytes".to_string(),
                "Address".to_string(),
                "BigInt".to_string(),
            ],
            GRAPH_TS_MODULE,
        )]
    }

    /// Generate all types from the ABI.
    pub fn generate_types(&self) -> Vec<Class> {
        let mut classes = Vec::new();
        classes.extend(self.generate_event_types());
        classes.extend(self.generate_smart_contract_class());
        classes.extend(self.generate_call_types());
        classes
    }

    /// Generate event type classes.
    fn generate_event_types(&self) -> Vec<Class> {
        let mut classes = Vec::new();
        let events = self.disambiguate_events();

        for (event, alias) in events {
            let event_class_name = alias.clone();
            let mut tuple_classes = Vec::new();

            // Generate params class
            let params_class_name = [&event_class_name, "__Params"].concat();
            let mut params_class = ts::klass(&params_class_name).exported();
            params_class.add_member(ClassMember::new("_event", &event_class_name));
            params_class.add_method(Method::new(
                "constructor",
                vec![TsParam::new("event", ts::NamedType::new(&event_class_name))],
                None,
                "this._event = event",
            ));

            // Generate getters for event params
            let inputs = self.disambiguate_params(&event.inputs, "param");
            for (index, (param, param_name)) in inputs.iter().enumerate() {
                let param_object = self.generate_event_param(
                    param,
                    param_name,
                    index,
                    &event_class_name,
                    &mut tuple_classes,
                );
                params_class.add_method(param_object);
            }

            // Generate event class
            let mut event_class = ts::klass(&event_class_name)
                .exported()
                .extends("ethereum.Event");
            event_class.add_method(Method::new(
                "get params",
                vec![],
                Some(ts::NamedType::new(&params_class_name).into()),
                format!("return new {}(this)", params_class_name),
            ));

            classes.push(event_class);
            classes.push(params_class);
            classes.extend(tuple_classes);
        }

        classes
    }

    /// Generate the smart contract class with call methods.
    fn generate_smart_contract_class(&self) -> Vec<Class> {
        let mut classes = Vec::new();

        let mut contract_class = ts::klass(&self.name)
            .exported()
            .extends("ethereum.SmartContract");

        // Add static bind method
        let contract_name = &self.name;
        contract_class.add_static_method(ts::StaticMethod::new(
            "bind",
            vec![TsParam::new("address", ts::NamedType::new("Address"))],
            ts::NamedType::new(&self.name),
            format!("return new {}('{}', address)", contract_name, contract_name),
        ));

        // Get callable functions and sort alphabetically for deterministic output
        let mut functions = self.get_callable_functions();
        functions.sort_by(|a, b| a.name.cmp(&b.name));
        let disambiguated = self.disambiguate_functions(&functions);

        for (func, alias) in disambiguated {
            let (method, try_method, result_classes) = self.generate_function_methods(func, &alias);
            contract_class.add_method(method);
            contract_class.add_method(try_method);
            classes.extend(result_classes);
        }

        classes.push(contract_class);
        classes
    }

    /// Generate call type classes.
    fn generate_call_types(&self) -> Vec<Class> {
        let mut classes = Vec::new();
        let mut functions = self.get_call_functions();
        functions.sort_by(|a, b| a.name.cmp(&b.name));
        let disambiguated = self.disambiguate_call_functions(&functions);

        for (func, alias) in disambiguated {
            let cap_alias = capitalize(&alias);
            let call_class_name = format!("{}Call", cap_alias);
            let mut tuple_classes = Vec::new();

            // Generate inputs class
            let inputs_class_name = [&call_class_name, "__Inputs"].concat();
            let mut inputs_class = ts::klass(&inputs_class_name).exported();
            inputs_class.add_member(ClassMember::new("_call", &call_class_name));
            inputs_class.add_method(Method::new(
                "constructor",
                vec![TsParam::new("call", ts::NamedType::new(&call_class_name))],
                None,
                "this._call = call",
            ));

            let inputs = self.disambiguate_params_from_func_inputs(&func.inputs, "value");
            for (index, (param, param_name)) in inputs.iter().enumerate() {
                let getter = self.generate_input_output_getter(
                    param,
                    param_name,
                    index,
                    &call_class_name,
                    "call",
                    "inputValues",
                    &mut tuple_classes,
                );
                inputs_class.add_method(getter);
            }

            // Generate outputs class
            let outputs_class_name = [&call_class_name, "__Outputs"].concat();
            let mut outputs_class = ts::klass(&outputs_class_name).exported();
            outputs_class.add_member(ClassMember::new("_call", &call_class_name));
            outputs_class.add_method(Method::new(
                "constructor",
                vec![TsParam::new("call", ts::NamedType::new(&call_class_name))],
                None,
                "this._call = call",
            ));

            let outputs = self.disambiguate_params_from_func_inputs(&func.outputs, "value");
            for (index, (param, param_name)) in outputs.iter().enumerate() {
                let getter = self.generate_input_output_getter(
                    param,
                    param_name,
                    index,
                    &call_class_name,
                    "call",
                    "outputValues",
                    &mut tuple_classes,
                );
                outputs_class.add_method(getter);
            }

            // Generate call class
            let mut call_class = ts::klass(&call_class_name)
                .exported()
                .extends("ethereum.Call");
            call_class.add_method(Method::new(
                "get inputs",
                vec![],
                Some(ts::NamedType::new(&inputs_class_name).into()),
                format!("return new {}(this)", inputs_class_name),
            ));
            call_class.add_method(Method::new(
                "get outputs",
                vec![],
                Some(ts::NamedType::new(&outputs_class_name).into()),
                format!("return new {}(this)", outputs_class_name),
            ));

            classes.push(call_class);
            classes.push(inputs_class);
            classes.push(outputs_class);
            classes.extend(tuple_classes);
        }

        classes
    }

    /// Generate a getter method for an event parameter.
    fn generate_event_param(
        &self,
        param: &EventParam,
        name: &str,
        index: usize,
        event_class_name: &str,
        tuple_classes: &mut Vec<Class>,
    ) -> Method {
        // Handle indexed params - strings, bytes and arrays are hashed to bytes32
        let value_type = if param.indexed {
            self.indexed_input_type(&param.kind)
        } else {
            param.kind.clone()
        };

        if self.contains_tuple_type(&value_type) {
            self.generate_tuple_getter(
                &param.kind,
                name,
                index,
                event_class_name,
                "event",
                "parameters",
                tuple_classes,
            )
        } else {
            let asc_type = self.asc_type_for_ethereum(&value_type);
            let access = format!("this._event.parameters[{}].value", index);
            let conversion = self.ethereum_to_asc(&access, &value_type, None);
            Method::new(
                format!("get {}", name),
                vec![],
                Some(ts::TypeExpr::Raw(asc_type)),
                format!("return {}", conversion),
            )
        }
    }

    /// Generate a getter for call inputs/outputs.
    #[allow(clippy::too_many_arguments)]
    fn generate_input_output_getter(
        &self,
        param: &Param,
        name: &str,
        index: usize,
        parent_class: &str,
        parent_type: &str,
        parent_field: &str,
        tuple_classes: &mut Vec<Class>,
    ) -> Method {
        if self.contains_tuple_type(&param.kind) {
            self.generate_tuple_getter(
                &param.kind,
                name,
                index,
                parent_class,
                parent_type,
                parent_field,
                tuple_classes,
            )
        } else {
            let asc_type = self.asc_type_for_ethereum(&param.kind);
            let access = format!("this._{}.{}[{}].value", parent_type, parent_field, index);
            let conversion = self.ethereum_to_asc(&access, &param.kind, None);
            Method::new(
                format!("get {}", name),
                vec![],
                Some(ts::TypeExpr::Raw(asc_type)),
                format!("return {}", conversion),
            )
        }
    }

    /// Generate a tuple getter and its associated classes.
    #[allow(clippy::too_many_arguments)]
    fn generate_tuple_getter(
        &self,
        param_type: &ParamType,
        name: &str,
        index: usize,
        parent_class: &str,
        parent_type: &str,
        parent_field: &str,
        tuple_classes: &mut Vec<Class>,
    ) -> Method {
        let cap_name = capitalize(name);
        let tuple_identifier = format!("{}{}", parent_class, cap_name);
        let tuple_class_name = if parent_field == "outputValues" {
            format!("{}OutputStruct", tuple_identifier)
        } else {
            format!("{}Struct", tuple_identifier)
        };

        let is_tuple = matches!(param_type, ParamType::Tuple(_));
        let access_code = if parent_type == "tuple" {
            format!("this[{}]", index)
        } else {
            format!("this._{}.{}[{}].value", parent_type, parent_field, index)
        };

        let return_value = self.ethereum_to_asc(&access_code, param_type, Some(&tuple_class_name));

        let return_type = if self.is_tuple_matrix_type(param_type) {
            format!("Array<Array<{}>>", tuple_class_name)
        } else if self.is_tuple_array_type(param_type) {
            format!("Array<{}>", tuple_class_name)
        } else {
            tuple_class_name.clone()
        };

        let body = if is_tuple {
            format!("return changetype<{}>({})", tuple_class_name, return_value)
        } else {
            format!("return {}", return_value)
        };

        // Generate tuple class
        if let Some(components) = self.get_tuple_components(param_type) {
            let mut tuple_class = ts::klass(&tuple_class_name)
                .exported()
                .extends("ethereum.Tuple");

            let component_params = self.disambiguate_tuple_components(components);
            for (idx, (component, component_name)) in component_params.iter().enumerate() {
                let component_getter = self.generate_tuple_component_getter(
                    component,
                    component_name,
                    idx,
                    &tuple_identifier,
                    tuple_classes,
                );
                tuple_class.add_method(component_getter);
            }

            tuple_classes.push(tuple_class);
        }

        Method::new(
            format!("get {}", name),
            vec![],
            Some(ts::TypeExpr::Raw(return_type)),
            body,
        )
    }

    /// Generate a getter for a tuple component.
    fn generate_tuple_component_getter(
        &self,
        param_type: &ParamType,
        name: &str,
        index: usize,
        parent_class: &str,
        tuple_classes: &mut Vec<Class>,
    ) -> Method {
        if self.contains_tuple_type(param_type) {
            self.generate_tuple_getter(
                param_type,
                name,
                index,
                parent_class,
                "tuple",
                "",
                tuple_classes,
            )
        } else {
            let asc_type = self.asc_type_for_ethereum(param_type);
            let access = format!("this[{}]", index);
            let conversion = self.ethereum_to_asc(&access, param_type, None);
            Method::new(
                format!("get {}", name),
                vec![],
                Some(ts::TypeExpr::Raw(asc_type)),
                format!("return {}", conversion),
            )
        }
    }

    /// Generate methods for a callable function.
    fn generate_function_methods(
        &self,
        func: &Function,
        alias: &str,
    ) -> (Method, Method, Vec<Class>) {
        let mut result_classes = Vec::new();
        let fn_signature = self.function_signature(func);
        let contract_name = &self.name;
        let tuple_result_parent_type = [contract_name, "__", alias, "Result"].concat();
        let tuple_input_parent_type = [contract_name, "__", alias, "Input"].concat();

        // Disambiguate outputs
        let outputs = self.disambiguate_params_from_func_inputs(&func.outputs, "value");

        // Determine return type
        let (return_type, simple_return_type) = if outputs.len() > 1 {
            // Multiple outputs - create a result struct
            let result_class = self.generate_result_class(
                &outputs,
                &tuple_result_parent_type,
                &mut result_classes,
            );
            result_classes.push(result_class.clone());
            (result_class.name.clone(), false)
        } else if !outputs.is_empty() {
            let (param, _) = &outputs[0];
            if self.contains_tuple_type(&param.kind) {
                let tuple_name = self.generate_tuple_return_type(
                    &param.kind,
                    0,
                    &tuple_result_parent_type,
                    &mut result_classes,
                );
                (tuple_name, true)
            } else {
                (self.asc_type_for_ethereum(&param.kind), true)
            }
        } else {
            ("void".to_string(), true)
        };

        // Disambiguate inputs
        let inputs = self.disambiguate_params_from_func_inputs(&func.inputs, "param");

        // Generate tuple types for inputs
        for (index, (param, _)) in inputs.iter().enumerate() {
            if self.contains_tuple_type(&param.kind) {
                self.generate_tuple_class_for_input(
                    &param.kind,
                    index,
                    &tuple_input_parent_type,
                    &mut result_classes,
                );
            }
        }

        // Build params
        let params: Vec<TsParam> = inputs
            .iter()
            .enumerate()
            .map(|(index, (param, name))| {
                let param_type =
                    self.get_param_type_for_input(&param.kind, index, &tuple_input_parent_type);
                TsParam::new(name.clone(), ts::TypeExpr::Raw(param_type))
            })
            .collect();

        // Build call arguments
        let call_args: Vec<String> = inputs
            .iter()
            .map(|(param, name)| self.ethereum_from_asc(name, &param.kind))
            .collect();

        let func_name = &func.name;
        let call_args_str = call_args.join(", ");
        let super_inputs = format!("'{}', '{}', [{}]", func_name, fn_signature, call_args_str);

        // Generate method body
        let method_body = self.generate_call_body(
            &outputs,
            &return_type,
            simple_return_type,
            &super_inputs,
            &tuple_result_parent_type,
            false,
        );

        let try_method_body = self.generate_call_body(
            &outputs,
            &return_type,
            simple_return_type,
            &super_inputs,
            &tuple_result_parent_type,
            true,
        );

        let method = Method::new(
            alias.to_string(),
            params.clone(),
            Some(ts::TypeExpr::Raw(return_type.clone())),
            method_body,
        );

        let try_method = Method::new(
            format!("try_{}", alias),
            params,
            Some(ts::TypeExpr::Raw(format!(
                "ethereum.CallResult<{}>",
                return_type
            ))),
            try_method_body,
        );

        (method, try_method, result_classes)
    }

    /// Generate call method body.
    fn generate_call_body(
        &self,
        outputs: &[(&Param, String)],
        return_type: &str,
        simple_return_type: bool,
        super_inputs: &str,
        tuple_result_parent_type: &str,
        is_try: bool,
    ) -> String {
        let nl = "\n";
        let (call_stmt, result_var) = if is_try {
            let mut lines = Vec::new();
            lines.push(format!("let result = super.tryCall({})", super_inputs));
            lines.push("    if (result.reverted) {".to_string());
            lines.push("      return new ethereum.CallResult()".to_string());
            lines.push("    }".to_string());
            lines.push("    let value = result.value".to_string());
            (lines.join(nl), "value")
        } else {
            (
                format!("let result = super.call({})", super_inputs),
                "result",
            )
        };

        let return_val = if simple_return_type {
            if outputs.is_empty() {
                String::new()
            } else {
                let (param, _) = &outputs[0];
                let tuple_name = if self.is_tuple_array_type(&param.kind) {
                    Some(self.tuple_type_name(&param.kind, 0, tuple_result_parent_type))
                } else {
                    None
                };
                let val = self.ethereum_to_asc(
                    &format!("{}[0]", result_var),
                    &param.kind,
                    tuple_name.as_deref(),
                );
                if matches!(param.kind, ParamType::Tuple(_)) {
                    format!("changetype<{}>({})", return_type, val)
                } else {
                    val
                }
            }
        } else {
            let conversions: Vec<String> = outputs
                .iter()
                .enumerate()
                .map(|(index, (param, _))| {
                    let tuple_name = if self.is_tuple_array_type(&param.kind) {
                        Some(self.tuple_type_name(&param.kind, index, tuple_result_parent_type))
                    } else {
                        None
                    };
                    let val = self.ethereum_to_asc(
                        &format!("{}[{}]", result_var, index),
                        &param.kind,
                        tuple_name.as_deref(),
                    );
                    if matches!(param.kind, ParamType::Tuple(_)) {
                        let tn = self.tuple_type_name(&param.kind, index, tuple_result_parent_type);
                        format!("changetype<{}>({})", tn, val)
                    } else {
                        val
                    }
                })
                .collect();
            let conv_str = conversions.join(", ");
            format!("new {}({})", return_type, conv_str)
        };

        if is_try {
            [
                &call_stmt,
                nl,
                "    return ethereum.CallResult.fromValue(",
                &return_val,
                ")",
            ]
            .concat()
        } else if outputs.is_empty() {
            call_stmt
        } else {
            [&call_stmt, nl, nl, "    return (", &return_val, ")"].concat()
        }
    }

    /// Generate a result class for multiple outputs.
    fn generate_result_class(
        &self,
        outputs: &[(&Param, String)],
        tuple_result_parent_type: &str,
        result_classes: &mut Vec<Class>,
    ) -> Class {
        let class_name = tuple_result_parent_type.to_string();
        let mut klass = ts::klass(&class_name).exported();

        // Add constructor
        let constructor_params: Vec<TsParam> = outputs
            .iter()
            .enumerate()
            .map(|(index, (param, _))| {
                let param_type =
                    self.get_param_type_for_input(&param.kind, index, tuple_result_parent_type);
                TsParam::new(format!("value{}", index), ts::TypeExpr::Raw(param_type))
            })
            .collect();

        let nl = "\n";
        let constructor_body: Vec<String> = outputs
            .iter()
            .enumerate()
            .map(|(index, _)| format!("this.value{} = value{}", index, index))
            .collect();

        klass.add_method(Method::new(
            "constructor",
            constructor_params,
            None,
            constructor_body.join(&format!("{}    ", nl)),
        ));

        // Add toMap method
        let map_entries: Vec<String> = outputs
            .iter()
            .enumerate()
            .map(|(index, (param, _))| {
                let this_val = format!("this.value{}", index);
                let from_asc = self.ethereum_from_asc(&this_val, &param.kind);
                format!("map.set('value{}', {})", index, from_asc)
            })
            .collect();

        let map_body = [
            "let map = new TypedMap<string,ethereum.Value>()",
            nl,
            "    ",
            &map_entries.join(&format!("{}    ", nl)),
            nl,
            "    return map",
        ]
        .concat();

        klass.add_method(Method::new(
            "toMap",
            vec![],
            Some(ts::TypeExpr::Raw(
                "TypedMap<string,ethereum.Value>".to_string(),
            )),
            map_body,
        ));

        // Add members
        for (index, (param, _)) in outputs.iter().enumerate() {
            let param_type =
                self.get_param_type_for_input(&param.kind, index, tuple_result_parent_type);
            klass.add_member(ClassMember::new(format!("value{}", index), param_type));
        }

        // Add getters for outputs
        // If an output has a name, generate a getter like getName()
        // If an output has no name (empty or just whitespace), generate getValue{index}()
        for (index, (param, _)) in outputs.iter().enumerate() {
            let getter_name = if param.name.trim().is_empty() {
                // Unnamed output: getValue0(), getValue1(), etc.
                format!("getValue{}", index)
            } else {
                // Named output: getOwner(), getDisplayName(), etc.
                let cap = capitalize(&param.name);
                format!("get{}", cap)
            };
            let param_type =
                self.get_param_type_for_input(&param.kind, index, tuple_result_parent_type);
            klass.add_method(Method::new(
                getter_name,
                vec![],
                Some(ts::TypeExpr::Raw(param_type)),
                format!("return this.value{}", index),
            ));
        }

        // Generate tuple classes for outputs
        for (index, (param, _)) in outputs.iter().enumerate() {
            if self.contains_tuple_type(&param.kind) {
                self.generate_tuple_class_for_input(
                    &param.kind,
                    index,
                    tuple_result_parent_type,
                    result_classes,
                );
            }
        }

        klass
    }

    /// Generate tuple return type name and classes.
    fn generate_tuple_return_type(
        &self,
        param_type: &ParamType,
        index: usize,
        parent_type: &str,
        result_classes: &mut Vec<Class>,
    ) -> String {
        self.generate_tuple_class_for_input(param_type, index, parent_type, result_classes);
        let tuple_name = self.tuple_type_name(param_type, index, parent_type);
        if self.is_tuple_array_type(param_type) {
            format!("Array<{}>", tuple_name)
        } else if self.is_tuple_matrix_type(param_type) {
            format!("Array<Array<{}>>", tuple_name)
        } else {
            tuple_name
        }
    }

    /// Generate tuple class for an input/output.
    fn generate_tuple_class_for_input(
        &self,
        param_type: &ParamType,
        index: usize,
        parent_type: &str,
        result_classes: &mut Vec<Class>,
    ) {
        let tuple_class_name = self.tuple_type_name(param_type, index, parent_type);
        let mut tuple_class = ts::klass(&tuple_class_name)
            .exported()
            .extends("ethereum.Tuple");

        if let Some(components) = self.get_tuple_components(param_type) {
            let component_params = self.disambiguate_tuple_components(components);
            for (idx, (component, component_name)) in component_params.iter().enumerate() {
                let getter = if self.contains_tuple_type(component) {
                    // Recursively generate tuple classes
                    let cap = capitalize(&format!("{}", index));
                    let nested_parent = format!("{}Value{}", parent_type, cap);
                    self.generate_tuple_class_for_input(
                        component,
                        idx,
                        &nested_parent,
                        result_classes,
                    );
                    let nested_tuple_name = self.tuple_type_name(component, idx, &nested_parent);
                    let access = format!("this[{}]", idx);
                    let conversion =
                        self.ethereum_to_asc(&access, component, Some(&nested_tuple_name));
                    let return_type = if self.is_tuple_array_type(component) {
                        format!("Array<{}>", nested_tuple_name)
                    } else {
                        nested_tuple_name.clone()
                    };
                    let body = if matches!(component, ParamType::Tuple(_)) {
                        format!("return changetype<{}>({})", nested_tuple_name, conversion)
                    } else {
                        format!("return {}", conversion)
                    };
                    Method::new(
                        format!("get {}", component_name),
                        vec![],
                        Some(ts::TypeExpr::Raw(return_type)),
                        body,
                    )
                } else {
                    let asc_type = self.asc_type_for_ethereum(component);
                    let access = format!("this[{}]", idx);
                    let conversion = self.ethereum_to_asc(&access, component, None);
                    Method::new(
                        format!("get {}", component_name),
                        vec![],
                        Some(ts::TypeExpr::Raw(asc_type)),
                        format!("return {}", conversion),
                    )
                };
                tuple_class.add_method(getter);
            }
        }

        result_classes.push(tuple_class);
    }

    /// Get tuple type name for a param.
    fn tuple_type_name(&self, _param_type: &ParamType, index: usize, parent_type: &str) -> String {
        format!("{}Value{}Struct", parent_type, index)
    }

    /// Get the param type string for an input, handling tuples.
    fn get_param_type_for_input(
        &self,
        param_type: &ParamType,
        index: usize,
        parent_type: &str,
    ) -> String {
        if matches!(param_type, ParamType::Tuple(_)) {
            self.tuple_type_name(param_type, index, parent_type)
        } else if self.is_tuple_matrix_type(param_type) {
            let tn = self.tuple_type_name(param_type, index, parent_type);
            format!("Array<Array<{}>>", tn)
        } else if self.is_tuple_array_type(param_type) {
            let tn = self.tuple_type_name(param_type, index, parent_type);
            format!("Array<{}>", tn)
        } else {
            self.asc_type_for_ethereum(param_type)
        }
    }

    /// Get callable functions (view, pure, nonpayable, constant with outputs).
    fn get_callable_functions(&self) -> Vec<&Function> {
        self.contract
            .functions()
            .filter(|f| {
                !f.outputs.is_empty()
                    && matches!(
                        f.state_mutability,
                        StateMutability::View | StateMutability::Pure | StateMutability::NonPayable
                    )
            })
            .collect()
    }

    /// Get functions that can be used as calls (non-view, non-pure functions).
    fn get_call_functions(&self) -> Vec<&Function> {
        self.contract
            .functions()
            .filter(|f| {
                matches!(
                    f.state_mutability,
                    StateMutability::NonPayable | StateMutability::Payable
                )
            })
            .collect()
    }

    /// Disambiguate events with duplicate names.
    fn disambiguate_events(&self) -> Vec<(&Event, String)> {
        let mut result = Vec::new();
        let mut collision_counter: HashMap<String, u32> = HashMap::new();

        for event in self.contract.events() {
            let name = handle_reserved_word(&event.name);
            let counter = collision_counter.entry(name.clone()).or_insert(0);
            let alias = if *counter == 0 {
                name.clone()
            } else {
                format!("{}{}", name, counter)
            };
            *counter += 1;
            result.push((event, alias));
        }

        result
    }

    /// Disambiguate functions.
    fn disambiguate_functions<'a>(
        &self,
        functions: &[&'a Function],
    ) -> Vec<(&'a Function, String)> {
        let mut result = Vec::new();
        let mut collision_counter: HashMap<String, u32> = HashMap::new();

        for func in functions {
            let name = handle_reserved_word(&func.name);
            let counter = collision_counter.entry(name.clone()).or_insert(0);
            let alias = if *counter == 0 {
                name.clone()
            } else {
                format!("{}{}", name, counter)
            };
            *counter += 1;
            result.push((*func, alias));
        }

        result
    }

    /// Disambiguate call functions.
    fn disambiguate_call_functions<'a>(
        &self,
        functions: &[&'a Function],
    ) -> Vec<(&'a Function, String)> {
        let mut result = Vec::new();
        let mut collision_counter: HashMap<String, u32> = HashMap::new();

        for func in functions {
            let name = if func.name.is_empty() {
                "default".to_string()
            } else {
                handle_reserved_word(&func.name)
            };
            let counter = collision_counter.entry(name.clone()).or_insert(0);
            let alias = if *counter == 0 {
                name.clone()
            } else {
                format!("{}{}", name, counter)
            };
            *counter += 1;
            result.push((*func, alias));
        }

        result
    }

    /// Disambiguate event params.
    fn disambiguate_params<'a>(
        &self,
        params: &'a [EventParam],
        default_prefix: &str,
    ) -> Vec<(&'a EventParam, String)> {
        let mut result = Vec::new();
        let mut collision_counter: HashMap<String, u32> = HashMap::new();

        for (index, param) in params.iter().enumerate() {
            let name = if param.name.is_empty() {
                format!("{}{}", default_prefix, index)
            } else {
                handle_reserved_word(&param.name)
            };
            let counter = collision_counter.entry(name.clone()).or_insert(0);
            let disambiguated = if *counter == 0 {
                name.clone()
            } else {
                format!("{}{}", name, counter)
            };
            *counter += 1;
            result.push((param, disambiguated));
        }

        result
    }

    /// Disambiguate function params.
    fn disambiguate_params_from_func_inputs<'a>(
        &self,
        params: &'a [Param],
        default_prefix: &str,
    ) -> Vec<(&'a Param, String)> {
        let mut result = Vec::new();
        let mut collision_counter: HashMap<String, u32> = HashMap::new();

        for (index, param) in params.iter().enumerate() {
            let name = if param.name.is_empty() {
                format!("{}{}", default_prefix, index)
            } else {
                handle_reserved_word(&param.name)
            };
            let counter = collision_counter.entry(name.clone()).or_insert(0);
            let disambiguated = if *counter == 0 {
                name.clone()
            } else {
                format!("{}{}", name, counter)
            };
            *counter += 1;
            result.push((param, disambiguated));
        }

        result
    }

    /// Disambiguate tuple components.
    fn disambiguate_tuple_components<'a>(
        &self,
        components: &'a [ParamType],
    ) -> Vec<(&'a ParamType, String)> {
        components
            .iter()
            .enumerate()
            .map(|(index, component)| (component, format!("value{}", index)))
            .collect()
    }

    /// Get function signature with return types.
    /// Format: `name(input_types):(output_types)`
    fn function_signature(&self, func: &Function) -> String {
        let input_types: Vec<String> = func.inputs.iter().map(|p| p.kind.to_string()).collect();
        let output_types: Vec<String> = func.outputs.iter().map(|p| p.kind.to_string()).collect();
        let name = &func.name;
        let inputs = input_types.join(",");
        let outputs = output_types.join(",");
        format!("{}({}):({})", name, inputs, outputs)
    }

    /// Get AssemblyScript type for an Ethereum type.
    fn asc_type_for_ethereum(&self, param_type: &ParamType) -> String {
        match param_type {
            ParamType::Address => "Address".to_string(),
            ParamType::Bool => "boolean".to_string(),
            ParamType::Bytes => "Bytes".to_string(),
            ParamType::FixedBytes(_) => "Bytes".to_string(),
            ParamType::Int(bits) => {
                if *bits <= 32 {
                    "i32".to_string()
                } else {
                    "BigInt".to_string()
                }
            }
            ParamType::Uint(bits) => {
                if *bits <= 24 {
                    "i32".to_string()
                } else {
                    "BigInt".to_string()
                }
            }
            ParamType::String => "string".to_string(),
            ParamType::Array(inner) => {
                let inner_type = self.asc_type_for_ethereum(inner);
                format!("Array<{}>", inner_type)
            }
            ParamType::FixedArray(inner, _) => {
                let inner_type = self.asc_type_for_ethereum(inner);
                format!("Array<{}>", inner_type)
            }
            ParamType::Tuple(_) => "ethereum.Tuple".to_string(),
        }
    }

    /// Convert ethereum value to AssemblyScript.
    fn ethereum_to_asc(
        &self,
        code: &str,
        param_type: &ParamType,
        tuple_type: Option<&str>,
    ) -> String {
        match param_type {
            ParamType::Address => format!("{}.toAddress()", code),
            ParamType::Bool => format!("{}.toBoolean()", code),
            ParamType::Bytes | ParamType::FixedBytes(_) => format!("{}.toBytes()", code),
            ParamType::Int(bits) => {
                if *bits <= 32 {
                    format!("{}.toI32()", code)
                } else {
                    format!("{}.toBigInt()", code)
                }
            }
            ParamType::Uint(bits) => {
                if *bits <= 24 {
                    format!("{}.toI32()", code)
                } else {
                    format!("{}.toBigInt()", code)
                }
            }
            ParamType::String => format!("{}.toString()", code),
            ParamType::Array(inner) | ParamType::FixedArray(inner, _) => match inner.as_ref() {
                ParamType::Address => format!("{}.toAddressArray()", code),
                ParamType::Bool => format!("{}.toBooleanArray()", code),
                ParamType::Bytes | ParamType::FixedBytes(_) => {
                    format!("{}.toBytesArray()", code)
                }
                ParamType::Int(bits) => {
                    if *bits <= 32 {
                        format!("{}.toI32Array()", code)
                    } else {
                        format!("{}.toBigIntArray()", code)
                    }
                }
                ParamType::Uint(bits) => {
                    if *bits <= 24 {
                        format!("{}.toI32Array()", code)
                    } else {
                        format!("{}.toBigIntArray()", code)
                    }
                }
                ParamType::String => format!("{}.toStringArray()", code),
                ParamType::Tuple(_) => {
                    if let Some(tuple_name) = tuple_type {
                        format!("{}.toTupleArray<{}>()", code, tuple_name)
                    } else {
                        format!("{}.toTupleArray<ethereum.Tuple>()", code)
                    }
                }
                ParamType::Array(inner2) | ParamType::FixedArray(inner2, _) => {
                    self.ethereum_to_asc_matrix(code, inner2.as_ref(), tuple_type)
                }
            },
            ParamType::Tuple(_) => format!("{}.toTuple()", code),
        }
    }

    /// Convert matrix type to AssemblyScript.
    fn ethereum_to_asc_matrix(
        &self,
        code: &str,
        inner_type: &ParamType,
        tuple_type: Option<&str>,
    ) -> String {
        match inner_type {
            ParamType::Address => format!("{}.toAddressMatrix()", code),
            ParamType::Bool => format!("{}.toBooleanMatrix()", code),
            ParamType::Bytes | ParamType::FixedBytes(_) => format!("{}.toBytesMatrix()", code),
            ParamType::Int(bits) => {
                if *bits <= 32 {
                    format!("{}.toI32Matrix()", code)
                } else {
                    format!("{}.toBigIntMatrix()", code)
                }
            }
            ParamType::Uint(bits) => {
                if *bits <= 24 {
                    format!("{}.toI32Matrix()", code)
                } else {
                    format!("{}.toBigIntMatrix()", code)
                }
            }
            ParamType::String => format!("{}.toStringMatrix()", code),
            ParamType::Tuple(_) => {
                if let Some(tuple_name) = tuple_type {
                    format!("{}.toTupleMatrix<{}>()", code, tuple_name)
                } else {
                    format!("{}.toTupleMatrix<ethereum.Tuple>()", code)
                }
            }
            _ => format!("{}.toStringMatrix()", code), // fallback
        }
    }

    /// Convert AssemblyScript value to ethereum value.
    fn ethereum_from_asc(&self, code: &str, param_type: &ParamType) -> String {
        match param_type {
            ParamType::Address => format!("ethereum.Value.fromAddress({})", code),
            ParamType::Bool => format!("ethereum.Value.fromBoolean({})", code),
            ParamType::Bytes => format!("ethereum.Value.fromBytes({})", code),
            ParamType::FixedBytes(_) => format!("ethereum.Value.fromFixedBytes({})", code),
            ParamType::Int(bits) => {
                if *bits <= 32 {
                    format!("ethereum.Value.fromI32({})", code)
                } else {
                    format!("ethereum.Value.fromSignedBigInt({})", code)
                }
            }
            ParamType::Uint(bits) => {
                if *bits <= 24 {
                    format!(
                        "ethereum.Value.fromUnsignedBigInt(BigInt.fromI32({}))",
                        code
                    )
                } else {
                    format!("ethereum.Value.fromUnsignedBigInt({})", code)
                }
            }
            ParamType::String => format!("ethereum.Value.fromString({})", code),
            ParamType::Array(inner) | ParamType::FixedArray(inner, _) => {
                self.ethereum_from_asc_array(code, inner.as_ref())
            }
            ParamType::Tuple(_) => format!("ethereum.Value.fromTuple({})", code),
        }
    }

    /// Convert array to ethereum value.
    fn ethereum_from_asc_array(&self, code: &str, inner_type: &ParamType) -> String {
        match inner_type {
            ParamType::Address => format!("ethereum.Value.fromAddressArray({})", code),
            ParamType::Bool => format!("ethereum.Value.fromBooleanArray({})", code),
            ParamType::Bytes => format!("ethereum.Value.fromBytesArray({})", code),
            ParamType::FixedBytes(_) => format!("ethereum.Value.fromFixedBytesArray({})", code),
            ParamType::Int(bits) => {
                if *bits <= 32 {
                    format!("ethereum.Value.fromI32Array({})", code)
                } else {
                    format!("ethereum.Value.fromSignedBigIntArray({})", code)
                }
            }
            ParamType::Uint(bits) => {
                if *bits <= 24 {
                    format!("ethereum.Value.fromI32Array({})", code)
                } else {
                    format!("ethereum.Value.fromUnsignedBigIntArray({})", code)
                }
            }
            ParamType::String => format!("ethereum.Value.fromStringArray({})", code),
            ParamType::Tuple(_) => format!("ethereum.Value.fromTupleArray({})", code),
            ParamType::Array(inner2) | ParamType::FixedArray(inner2, _) => {
                self.ethereum_from_asc_matrix(code, inner2.as_ref())
            }
        }
    }

    /// Convert matrix to ethereum value.
    fn ethereum_from_asc_matrix(&self, code: &str, inner_type: &ParamType) -> String {
        match inner_type {
            ParamType::Address => format!("ethereum.Value.fromAddressMatrix({})", code),
            ParamType::Bool => format!("ethereum.Value.fromBooleanMatrix({})", code),
            ParamType::Bytes => format!("ethereum.Value.fromBytesMatrix({})", code),
            ParamType::FixedBytes(_) => format!("ethereum.Value.fromFixedBytesMatrix({})", code),
            ParamType::Int(bits) => {
                if *bits <= 32 {
                    format!("ethereum.Value.fromI32Matrix({})", code)
                } else {
                    format!("ethereum.Value.fromSignedBigIntMatrix({})", code)
                }
            }
            ParamType::Uint(bits) => {
                if *bits <= 24 {
                    format!("ethereum.Value.fromI32Matrix({})", code)
                } else {
                    format!("ethereum.Value.fromUnsignedBigIntMatrix({})", code)
                }
            }
            ParamType::String => format!("ethereum.Value.fromStringMatrix({})", code),
            ParamType::Tuple(_) => format!("ethereum.Value.fromTupleMatrix({})", code),
            _ => format!("ethereum.Value.fromStringMatrix({})", code), // fallback
        }
    }

    /// Check if param type contains a tuple.
    fn contains_tuple_type(&self, param_type: &ParamType) -> bool {
        match param_type {
            ParamType::Tuple(_) => true,
            ParamType::Array(inner) | ParamType::FixedArray(inner, _) => {
                self.contains_tuple_type(inner)
            }
            _ => false,
        }
    }

    /// Check if param type is a tuple array.
    fn is_tuple_array_type(&self, param_type: &ParamType) -> bool {
        matches!(
            param_type,
            ParamType::Array(inner) | ParamType::FixedArray(inner, _)
                if matches!(inner.as_ref(), ParamType::Tuple(_))
        )
    }

    /// Check if param type is a tuple matrix (2D array).
    fn is_tuple_matrix_type(&self, param_type: &ParamType) -> bool {
        match param_type {
            ParamType::Array(inner) | ParamType::FixedArray(inner, _) => {
                self.is_tuple_array_type(inner)
            }
            _ => false,
        }
    }

    /// Get tuple components.
    fn get_tuple_components<'a>(&self, param_type: &'a ParamType) -> Option<&'a [ParamType]> {
        match param_type {
            ParamType::Tuple(components) => Some(components),
            ParamType::Array(inner) | ParamType::FixedArray(inner, _) => {
                self.get_tuple_components(inner)
            }
            _ => None,
        }
    }

    /// Handle indexed input type conversion.
    fn indexed_input_type(&self, param_type: &ParamType) -> ParamType {
        // Strings, bytes, and arrays are encoded and hashed to bytes32
        match param_type {
            ParamType::String | ParamType::Bytes | ParamType::Tuple(_) => ParamType::FixedBytes(32),
            ParamType::Array(_) | ParamType::FixedArray(_, _) => ParamType::FixedBytes(32),
            _ => param_type.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_abi(json: &str) -> Contract {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn test_simple_event() {
        let abi_json = r#"[
            {
                "type": "event",
                "name": "Transfer",
                "inputs": [
                    {"name": "from", "type": "address", "indexed": true},
                    {"name": "to", "type": "address", "indexed": true},
                    {"name": "value", "type": "uint256", "indexed": false}
                ],
                "anonymous": false
            }
        ]"#;

        let contract = parse_abi(abi_json);
        let gen = AbiCodeGenerator::new(contract, "Token");
        let types = gen.generate_types();

        assert!(types.iter().any(|c| c.name == "Transfer"));
        assert!(types.iter().any(|c| c.name == "Transfer__Params"));
    }

    #[test]
    fn test_function_with_outputs() {
        let abi_json = r#"[
            {
                "type": "function",
                "name": "balanceOf",
                "inputs": [{"name": "owner", "type": "address"}],
                "outputs": [{"name": "", "type": "uint256"}],
                "stateMutability": "view"
            }
        ]"#;

        let contract = parse_abi(abi_json);
        let gen = AbiCodeGenerator::new(contract, "Token");
        let types = gen.generate_types();

        assert!(types.iter().any(|c| c.name == "Token"));
        let token_class = types.iter().find(|c| c.name == "Token").unwrap();

        assert!(token_class.methods.iter().any(|m| m.name == "balanceOf"));
        assert!(token_class
            .methods
            .iter()
            .any(|m| m.name == "try_balanceOf"));
    }

    #[test]
    fn test_asc_type_for_ethereum() {
        let gen = AbiCodeGenerator::new(Contract::default(), "Test");

        assert_eq!(gen.asc_type_for_ethereum(&ParamType::Address), "Address");
        assert_eq!(gen.asc_type_for_ethereum(&ParamType::Bool), "boolean");
        assert_eq!(gen.asc_type_for_ethereum(&ParamType::Uint(256)), "BigInt");
        assert_eq!(gen.asc_type_for_ethereum(&ParamType::Uint(8)), "i32");
        assert_eq!(gen.asc_type_for_ethereum(&ParamType::Int(32)), "i32");
        assert_eq!(gen.asc_type_for_ethereum(&ParamType::String), "string");
        assert_eq!(gen.asc_type_for_ethereum(&ParamType::Bytes), "Bytes");
    }

    #[test]
    fn test_name_sanitization() {
        let gen = AbiCodeGenerator::new(Contract::default(), "Test!Contract@Name");
        assert_eq!(gen.name, "Test_Contract_Name");
    }

    #[test]
    fn test_indexed_input_type() {
        let gen = AbiCodeGenerator::new(Contract::default(), "Test");

        assert_eq!(
            gen.indexed_input_type(&ParamType::String),
            ParamType::FixedBytes(32)
        );
        assert_eq!(
            gen.indexed_input_type(&ParamType::Bytes),
            ParamType::FixedBytes(32)
        );
        assert_eq!(
            gen.indexed_input_type(&ParamType::Array(Box::new(ParamType::Uint(256)))),
            ParamType::FixedBytes(32)
        );
        assert_eq!(
            gen.indexed_input_type(&ParamType::Address),
            ParamType::Address
        );
        assert_eq!(
            gen.indexed_input_type(&ParamType::Uint(256)),
            ParamType::Uint(256)
        );
    }

    /// Test that overloaded events (same name, different inputs) are disambiguated.
    /// The TS CLI generates unique names like Transfer, Transfer1, Transfer2.
    #[test]
    fn test_overloaded_events() {
        let abi_json = r#"[
            {
                "type": "event",
                "name": "Transfer",
                "inputs": [],
                "anonymous": false
            },
            {
                "type": "event",
                "name": "Transfer",
                "inputs": [{"name": "to", "type": "address", "indexed": false}],
                "anonymous": false
            },
            {
                "type": "event",
                "name": "Transfer",
                "inputs": [
                    {"name": "from", "type": "address", "indexed": false},
                    {"name": "to", "type": "address", "indexed": false}
                ],
                "anonymous": false
            }
        ]"#;

        let contract = parse_abi(abi_json);
        let gen = AbiCodeGenerator::new(contract, "Token");
        let types = gen.generate_types();

        // Get all event class names
        let event_names: Vec<&str> = types
            .iter()
            .filter(|c| c.extends == Some("ethereum.Event".to_string()))
            .map(|c| c.name.as_str())
            .collect();

        // Verify we have 3 distinct Transfer events with disambiguation
        assert_eq!(
            event_names.len(),
            3,
            "Should have 3 Transfer event variants"
        );

        // Check that Transfer (with no suffix) exists
        assert!(
            event_names.contains(&"Transfer"),
            "Should have base Transfer event"
        );

        // Check that numbered variants exist (Transfer1, Transfer2)
        assert!(
            event_names.contains(&"Transfer1"),
            "Should have Transfer1 event"
        );
        assert!(
            event_names.contains(&"Transfer2"),
            "Should have Transfer2 event"
        );
    }

    /// Test that overloaded functions are disambiguated.
    #[test]
    fn test_overloaded_functions() {
        let abi_json = r#"[
            {
                "type": "function",
                "name": "getSomething",
                "inputs": [],
                "outputs": [{"name": "result", "type": "bytes32"}],
                "stateMutability": "view"
            },
            {
                "type": "function",
                "name": "getSomething",
                "inputs": [{"name": "owner", "type": "address"}],
                "outputs": [{"name": "result", "type": "bytes32"}],
                "stateMutability": "view"
            }
        ]"#;

        let contract = parse_abi(abi_json);
        let gen = AbiCodeGenerator::new(contract, "Token");
        let types = gen.generate_types();

        // Find the Token contract class
        let token_class = types.iter().find(|c| c.name == "Token").unwrap();

        // Get all method names (excluding try_ variants and bind)
        let method_names: Vec<&str> = token_class
            .methods
            .iter()
            .map(|m| m.name.as_str())
            .filter(|n| !n.starts_with("try_") && *n != "bind")
            .collect();

        // Verify we have disambiguated getSomething variants
        assert!(
            method_names.contains(&"getSomething"),
            "Should have base getSomething method"
        );
        assert!(
            method_names.contains(&"getSomething1"),
            "Should have getSomething1 method"
        );
    }

    /// Test that tuple/struct types in function inputs and outputs are handled correctly.
    #[test]
    fn test_tuple_types_in_functions() {
        let abi_json = r#"[
            {
                "type": "function",
                "name": "doSomething",
                "inputs": [
                    {
                        "name": "data",
                        "type": "tuple",
                        "components": [
                            {"name": "owner", "type": "address"},
                            {"name": "value", "type": "uint256"}
                        ]
                    }
                ],
                "outputs": [
                    {
                        "name": "result",
                        "type": "tuple",
                        "components": [
                            {"name": "success", "type": "bool"},
                            {"name": "newValue", "type": "uint256"}
                        ]
                    }
                ],
                "stateMutability": "nonpayable"
            }
        ]"#;

        let contract = parse_abi(abi_json);
        let gen = AbiCodeGenerator::new(contract, "TestContract");
        let types = gen.generate_types();

        // Get class names
        let class_names: Vec<&str> = types.iter().map(|c| c.name.as_str()).collect();

        // Should have main contract class
        assert!(
            class_names.contains(&"TestContract"),
            "Should have TestContract class"
        );

        // Should have struct classes for input and output tuples
        // Input struct: TestContract__doSomethingInputValue0Struct
        // Output struct: TestContract__doSomethingResultValue0Struct
        let has_input_struct = class_names
            .iter()
            .any(|n| n.contains("Input") && n.contains("Struct"));
        let has_output_struct = class_names
            .iter()
            .any(|n| n.contains("Result") && n.contains("Struct"));

        assert!(
            has_input_struct,
            "Should have input struct class, found: {:?}",
            class_names
        );
        assert!(
            has_output_struct,
            "Should have output/result struct class, found: {:?}",
            class_names
        );

        // Verify the output struct extends ethereum.Tuple
        let output_struct = types
            .iter()
            .find(|c| c.name.contains("Result") && c.name.contains("Struct"))
            .expect("Should find output struct");
        assert_eq!(
            output_struct.extends,
            Some("ethereum.Tuple".to_string()),
            "Output struct should extend ethereum.Tuple"
        );

        // Verify the output struct has getters for its components
        // The getters use "get valueN" naming for positional access to tuple elements
        let method_names: Vec<&str> = output_struct
            .methods
            .iter()
            .map(|m| m.name.as_str())
            .collect();
        assert!(
            method_names.iter().any(|n| n.contains("value0")),
            "Should have value0 getter for first component, found methods: {:?}",
            method_names
        );
        assert!(
            method_names.iter().any(|n| n.contains("value1")),
            "Should have value1 getter for second component, found methods: {:?}",
            method_names
        );
    }

    /// Test that tuple types in events are handled correctly.
    #[test]
    fn test_tuple_types_in_events() {
        let abi_json = r#"[
            {
                "type": "event",
                "name": "DataUpdated",
                "inputs": [
                    {"name": "id", "type": "uint256", "indexed": true},
                    {
                        "name": "data",
                        "type": "tuple",
                        "indexed": false,
                        "components": [
                            {"name": "timestamp", "type": "uint256"},
                            {"name": "value", "type": "bytes32"}
                        ]
                    }
                ],
                "anonymous": false
            }
        ]"#;

        let contract = parse_abi(abi_json);
        let gen = AbiCodeGenerator::new(contract, "TestContract");
        let types = gen.generate_types();

        // Get class names
        let class_names: Vec<&str> = types.iter().map(|c| c.name.as_str()).collect();

        // Should have event class
        assert!(
            class_names.contains(&"DataUpdated"),
            "Should have DataUpdated event class"
        );

        // Should have params class
        assert!(
            class_names.contains(&"DataUpdated__Params"),
            "Should have DataUpdated__Params class"
        );

        // Should have struct class for the tuple parameter
        let has_struct = class_names
            .iter()
            .any(|n| n.contains("Struct") && n.contains("DataUpdated"));
        assert!(
            has_struct,
            "Should have struct class for tuple parameter, found: {:?}",
            class_names
        );
    }

    /// Test that nested tuple types (struct with struct field) are handled.
    #[test]
    fn test_nested_tuple_types() {
        let abi_json = r#"[
            {
                "type": "function",
                "name": "getNestedData",
                "inputs": [],
                "outputs": [
                    {
                        "name": "result",
                        "type": "tuple",
                        "components": [
                            {"name": "id", "type": "uint256"},
                            {
                                "name": "inner",
                                "type": "tuple",
                                "components": [
                                    {"name": "x", "type": "uint256"},
                                    {"name": "y", "type": "uint256"}
                                ]
                            }
                        ]
                    }
                ],
                "stateMutability": "view"
            }
        ]"#;

        let contract = parse_abi(abi_json);
        let gen = AbiCodeGenerator::new(contract, "TestContract");
        let types = gen.generate_types();

        // Get class names
        let class_names: Vec<&str> = types.iter().map(|c| c.name.as_str()).collect();

        // Should have main contract class
        assert!(
            class_names.contains(&"TestContract"),
            "Should have TestContract class"
        );

        // Should have at least two struct classes (outer and inner)
        let struct_count = class_names.iter().filter(|n| n.contains("Struct")).count();
        assert!(
            struct_count >= 2,
            "Should have at least 2 struct classes for nested tuple, found {} in {:?}",
            struct_count,
            class_names
        );
    }

    /// Test that tuple arrays are handled correctly.
    #[test]
    fn test_tuple_array_types() {
        let abi_json = r#"[
            {
                "type": "function",
                "name": "getAllItems",
                "inputs": [],
                "outputs": [
                    {
                        "name": "items",
                        "type": "tuple[]",
                        "components": [
                            {"name": "id", "type": "uint256"},
                            {"name": "name", "type": "string"}
                        ]
                    }
                ],
                "stateMutability": "view"
            }
        ]"#;

        let contract = parse_abi(abi_json);
        let gen = AbiCodeGenerator::new(contract, "TestContract");
        let types = gen.generate_types();

        // Find the contract class
        let contract_class = types.iter().find(|c| c.name == "TestContract").unwrap();

        // Find the getAllItems method
        let method = contract_class
            .methods
            .iter()
            .find(|m| m.name == "getAllItems")
            .expect("Should have getAllItems method");

        // The return type should be an Array of the struct type
        let return_type = method
            .return_type
            .as_ref()
            .expect("Should have return type");
        let return_type_str = return_type.to_string();
        assert!(
            return_type_str.starts_with("Array<"),
            "Return type should be Array<...>, got: {}",
            return_type_str
        );
    }

    /// Test that array types in events are handled correctly.
    #[test]
    fn test_array_types_in_events() {
        let abi_json = r#"[
            {
                "type": "event",
                "name": "Airdropped",
                "inputs": [
                    {"name": "sender", "type": "address", "indexed": true},
                    {"name": "recipients", "type": "address[]", "indexed": false},
                    {"name": "amounts", "type": "uint256[]", "indexed": false}
                ],
                "anonymous": false
            }
        ]"#;

        let contract = parse_abi(abi_json);
        let gen = AbiCodeGenerator::new(contract, "Token");
        let types = gen.generate_types();

        // Find the params class
        let params_class = types
            .iter()
            .find(|c| c.name == "Airdropped__Params")
            .expect("Should have Airdropped__Params class");

        // Check that the array getters exist and have correct return types
        let method_names: Vec<&str> = params_class
            .methods
            .iter()
            .map(|m| m.name.as_str())
            .collect();

        assert!(
            method_names.iter().any(|n| n.contains("recipients")),
            "Should have recipients getter, found: {:?}",
            method_names
        );
        assert!(
            method_names.iter().any(|n| n.contains("amounts")),
            "Should have amounts getter, found: {:?}",
            method_names
        );

        // Verify the recipients getter returns Array<Address>
        let recipients_getter = params_class
            .methods
            .iter()
            .find(|m| m.name.contains("recipients"))
            .expect("Should have recipients getter");
        let return_type_str = recipients_getter
            .return_type
            .as_ref()
            .expect("Should have return type")
            .to_string();
        assert!(
            return_type_str.contains("Array<Address>"),
            "recipients should return Array<Address>, got: {}",
            return_type_str
        );

        // Verify the amounts getter returns Array<BigInt>
        let amounts_getter = params_class
            .methods
            .iter()
            .find(|m| m.name.contains("amounts"))
            .expect("Should have amounts getter");
        let amounts_type_str = amounts_getter
            .return_type
            .as_ref()
            .expect("Should have return type")
            .to_string();
        assert!(
            amounts_type_str.contains("Array<BigInt>"),
            "amounts should return Array<BigInt>, got: {}",
            amounts_type_str
        );
    }

    /// Test that 2D array types (matrices) are handled correctly in functions.
    #[test]
    fn test_matrix_types_in_functions() {
        let abi_json = r#"[
            {
                "type": "function",
                "name": "getMatrix",
                "inputs": [],
                "outputs": [
                    {"name": "data", "type": "uint256[][]"}
                ],
                "stateMutability": "view"
            }
        ]"#;

        let contract = parse_abi(abi_json);
        let gen = AbiCodeGenerator::new(contract, "TestContract");
        let types = gen.generate_types();

        // Find the contract class
        let contract_class = types.iter().find(|c| c.name == "TestContract").unwrap();

        // Find the getMatrix method
        let method = contract_class
            .methods
            .iter()
            .find(|m| m.name == "getMatrix")
            .expect("Should have getMatrix method");

        // The return type should be Array<Array<BigInt>>
        let return_type = method
            .return_type
            .as_ref()
            .expect("Should have return type");
        let return_type_str = return_type.to_string();
        assert!(
            return_type_str.contains("Array<Array<BigInt>>"),
            "Return type should be Array<Array<BigInt>>, got: {}",
            return_type_str
        );
    }
}
