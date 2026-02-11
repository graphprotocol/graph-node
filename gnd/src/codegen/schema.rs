//! Schema code generation.
//!
//! Generates AssemblyScript entity classes from GraphQL schemas.

use anyhow::{anyhow, Result};
use graphql_tools::parser::schema::{
    Definition, Document, Field, ObjectType, Type, TypeDefinition,
};

use super::types::{asc_type_for_value, value_from_asc, value_to_asc};
use super::typescript::{
    self as ts, ArrayType, Class, Method, ModuleImports, NamedType, NullableType, Param,
    StaticMethod, TypeExpr,
};
use crate::shared::handle_reserved_word;

/// Type of the ID field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdFieldKind {
    String,
    Bytes,
    Int8,
}

impl IdFieldKind {
    /// Get the AssemblyScript type name for this ID type.
    pub fn type_name(&self) -> &'static str {
        match self {
            IdFieldKind::String => "string",
            IdFieldKind::Bytes => "Bytes",
            IdFieldKind::Int8 => "i64",
        }
    }

    /// Get the GraphQL type name for this ID type.
    pub fn gql_type_name(&self) -> &'static str {
        match self {
            IdFieldKind::String => "String",
            IdFieldKind::Bytes => "Bytes",
            IdFieldKind::Int8 => "Int8",
        }
    }

    /// Get the code to create a Value from the ID.
    pub fn value_from(&self) -> &'static str {
        match self {
            IdFieldKind::String => "Value.fromString(id)",
            IdFieldKind::Bytes => "Value.fromBytes(id)",
            IdFieldKind::Int8 => "Value.fromI64(id)",
        }
    }

    /// Get the ValueKind for this ID type.
    pub fn value_kind(&self) -> &'static str {
        match self {
            IdFieldKind::String => "ValueKind.STRING",
            IdFieldKind::Bytes => "ValueKind.BYTES",
            IdFieldKind::Int8 => "ValueKind.INT8",
        }
    }

    /// Get the code to convert a Value to a string representation.
    pub fn value_to_string(&self) -> &'static str {
        match self {
            IdFieldKind::String => "id.toString()",
            IdFieldKind::Bytes => "id.toBytes().toHexString()",
            IdFieldKind::Int8 => "id.toI64().toString()",
        }
    }

    /// Get the code to convert the ID to a string.
    pub fn id_to_string_code(self) -> &'static str {
        match self {
            IdFieldKind::String => "id",
            IdFieldKind::Bytes => "id.toHexString()",
            IdFieldKind::Int8 => "id.toString()",
        }
    }

    /// Determine the ID field kind from a type name.
    pub fn from_type_name(type_name: &str) -> Self {
        match type_name {
            "Bytes" => IdFieldKind::Bytes,
            "Int8" => IdFieldKind::Int8,
            _ => IdFieldKind::String,
        }
    }

    /// Returns true if this ID type supports auto-increment (optional constructor arg).
    pub fn supports_auto(&self) -> bool {
        matches!(self, IdFieldKind::Int8 | IdFieldKind::Bytes)
    }

    /// Get the constructor parameter type (with nullability for auto-increment types).
    pub fn constructor_param_type(&self) -> &'static str {
        match self {
            IdFieldKind::String => "string",
            IdFieldKind::Bytes => "Bytes | null",
            IdFieldKind::Int8 => "i64", // primitive, can't be nullable
        }
    }

    /// Get the default value for constructor parameter (for auto-increment types).
    pub fn constructor_default(&self) -> Option<&'static str> {
        match self {
            IdFieldKind::String => None,
            IdFieldKind::Bytes => Some("null"),
            IdFieldKind::Int8 => Some("i64.MIN_VALUE"),
        }
    }

    /// Get the condition to check if id was provided (not auto-increment).
    pub fn auto_check_condition(&self) -> &'static str {
        match self {
            IdFieldKind::String => "", // N/A
            // For Bytes we end up generating 'if (id) { .. }' Using 'id !=
            // null' crashes the AssemblyScript compiler  0.19.23 because id
            // has type 'Bytes | null' and the compiler doesn't strip the
            // null from the type in the body of the if
            IdFieldKind::Bytes => "id",
            IdFieldKind::Int8 => "id != i64.MIN_VALUE",
        }
    }
}

/// Get the base type name from a GraphQL type (stripping NonNull and List wrappers).
fn get_base_type_name(ty: &Type<'_, String>) -> String {
    match ty {
        Type::NamedType(name) => name.clone(),
        Type::NonNullType(inner) => get_base_type_name(inner),
        Type::ListType(inner) => get_base_type_name(inner),
    }
}

/// Check if a type is nullable (not wrapped in NonNull).
fn is_nullable(ty: &Type<'_, String>) -> bool {
    !matches!(ty, Type::NonNullType(_))
}

/// Count the list nesting depth of a type.
/// `String` -> 0, `[String]` -> 1, `[[String]]` -> 2, etc.
fn list_depth(ty: &Type<'_, String>) -> u8 {
    match ty {
        Type::ListType(inner) => 1 + list_depth(inner),
        Type::NonNullType(inner) => list_depth(inner),
        Type::NamedType(_) => 0,
    }
}

/// Check if the innermost list members are nullable.
/// For `[String]` returns true, for `[String!]` returns false.
/// For non-list types, returns false.
fn is_list_member_nullable(ty: &Type<'_, String>) -> bool {
    match ty {
        Type::ListType(inner) => {
            // Check the immediate inner type
            is_nullable(inner)
        }
        Type::NonNullType(inner) => is_list_member_nullable(inner),
        Type::NamedType(_) => false,
    }
}

/// Check if a field has the @derivedFrom directive.
fn is_derived_field(field: &Field<'_, String>) -> bool {
    field.directives.iter().any(|d| d.name == "derivedFrom")
}

/// Check if an object type has the @entity directive.
fn is_entity_type(obj: &ObjectType<'_, String>) -> bool {
    obj.directives.iter().any(|d| d.name == "entity")
}

/// Collected entity info for code generation.
struct EntityInfo {
    name: String,
    id_kind: IdFieldKind,
    fields: Vec<FieldInfo>,
}

/// Collected field info.
struct FieldInfo {
    name: String,
    is_derived: bool,
    base_type: String,
    is_nullable: bool,
    /// The nesting depth of list wrappers. 0 = scalar, 1 = [T], 2 = [[T]], etc.
    list_depth: u8,
    /// Whether list members are nullable. Only meaningful when list_depth > 0.
    member_nullable: bool,
}

/// Schema code generator.
pub struct SchemaCodeGenerator {
    entities: Vec<EntityInfo>,
    entity_names: std::collections::HashSet<String>,
    /// Maps entity name to its ID field kind, for resolving entity reference types.
    entity_id_kinds: std::collections::HashMap<String, IdFieldKind>,
}

impl SchemaCodeGenerator {
    /// Create a new schema code generator from a parsed GraphQL document.
    ///
    /// Returns an error if the schema contains invalid patterns like non-nullable
    /// lists with nullable members (e.g., `[Something]!`).
    pub fn new(document: &Document<'_, String>) -> Result<Self> {
        let mut entities = Vec::new();
        let mut entity_names = std::collections::HashSet::new();
        let mut entity_id_kinds = std::collections::HashMap::new();

        // First pass: collect entity names and their ID types
        for def in &document.definitions {
            if let Definition::TypeDefinition(TypeDefinition::Object(obj)) = def {
                if is_entity_type(obj) {
                    entity_names.insert(obj.name.clone());
                    let id_field = obj.fields.iter().find(|f| f.name == "id");
                    let id_kind = id_field
                        .map(|f| IdFieldKind::from_type_name(&get_base_type_name(&f.field_type)))
                        .unwrap_or(IdFieldKind::String);
                    entity_id_kinds.insert(obj.name.clone(), id_kind);
                }
            }
        }

        // Second pass: collect entity info
        for def in &document.definitions {
            if let Definition::TypeDefinition(TypeDefinition::Object(obj)) = def {
                if is_entity_type(obj) {
                    let name = obj.name.clone();

                    // Find ID field
                    let id_field = obj.fields.iter().find(|f| f.name == "id");
                    let id_kind = id_field
                        .map(|f| IdFieldKind::from_type_name(&get_base_type_name(&f.field_type)))
                        .unwrap_or(IdFieldKind::String);

                    // Collect field info
                    let fields: Vec<_> = obj
                        .fields
                        .iter()
                        .map(|f| FieldInfo {
                            name: f.name.clone(),
                            is_derived: is_derived_field(f),
                            base_type: get_base_type_name(&f.field_type),
                            is_nullable: is_nullable(&f.field_type),
                            list_depth: list_depth(&f.field_type),
                            member_nullable: is_list_member_nullable(&f.field_type),
                        })
                        .collect();

                    entities.push(EntityInfo {
                        name,
                        id_kind,
                        fields,
                    });
                }
            }
        }

        // Validate: non-nullable lists must have non-nullable members
        for entity in &entities {
            for field in &entity.fields {
                if field.list_depth > 0 && !field.is_nullable && field.member_nullable {
                    return Err(anyhow!(
                        "Codegen can't generate code for GraphQL field '{}' of type '[{}]!' since the inner type is nullable.\n\
                         Suggestion: add an '!' to the inner type, e.g., '[{}!]!'",
                        field.name,
                        field.base_type,
                        field.base_type
                    ));
                }
            }
        }

        Ok(Self {
            entities,
            entity_names,
            entity_id_kinds,
        })
    }

    /// Generate module imports for the schema file.
    pub fn generate_module_imports(&self) -> Vec<ModuleImports> {
        vec![ModuleImports::new(
            vec![
                "TypedMap".to_string(),
                "Entity".to_string(),
                "Value".to_string(),
                "ValueKind".to_string(),
                "store".to_string(),
                "Bytes".to_string(),
                "BigInt".to_string(),
                "BigDecimal".to_string(),
                "Int8".to_string(),
            ],
            "@graphprotocol/graph-ts",
        )]
    }

    /// Generate entity classes from the schema.
    pub fn generate_types(&self, generate_store_methods: bool) -> Vec<Class> {
        self.entities
            .iter()
            .map(|entity| self.generate_entity_type(entity, generate_store_methods))
            .collect()
    }

    /// Generate derived loaders for fields with @derivedFrom.
    pub fn generate_derived_loaders(&self) -> Vec<Class> {
        let mut loaders = Vec::new();
        let mut seen_types = std::collections::HashSet::new();

        for entity in &self.entities {
            for field in &entity.fields {
                if field.is_derived && !seen_types.contains(&field.base_type) {
                    // Only generate loaders for entity types, not interfaces
                    if self.entity_names.contains(&field.base_type) {
                        seen_types.insert(field.base_type.clone());
                        loaders.push(self.generate_derived_loader(&field.base_type));
                    }
                }
            }
        }

        loaders
    }

    fn generate_entity_type(&self, entity: &EntityInfo, generate_store_methods: bool) -> Class {
        let mut klass = ts::klass(&entity.name).exported().extends("Entity");

        // Generate constructor
        klass.add_method(self.generate_constructor(&entity.id_kind));

        // Generate store methods
        if generate_store_methods {
            for method in self.generate_store_methods(&entity.name, &entity.id_kind) {
                match method {
                    StoreMethod::Regular(m) => klass.add_method(m),
                    StoreMethod::Static(m) => klass.add_static_method(m),
                }
            }
        }

        // Generate field getters and setters
        for field in &entity.fields {
            if let Some(getter) = self.generate_field_getter(&entity.name, field, &entity.id_kind) {
                klass.add_method(getter);
            }
            if let Some(setter) = self.generate_field_setter(field) {
                klass.add_method(setter);
            }
        }

        klass
    }

    fn generate_constructor(&self, id_kind: &IdFieldKind) -> Method {
        if id_kind.supports_auto() {
            // For Int8 and Bytes, make id optional to support auto-increment
            let param_type = id_kind.constructor_param_type();
            let default_value = id_kind.constructor_default().unwrap();
            let check_condition = id_kind.auto_check_condition();

            Method::new(
                "constructor",
                vec![Param::with_default(
                    "id",
                    TypeExpr::Raw(param_type.to_string()),
                    default_value,
                )],
                None,
                format!(
                    r#"
      super()
      if ({}) {{
        this.set('id', {})
      }}"#,
                    check_condition,
                    id_kind.value_from()
                ),
            )
            .with_doc("Leaving out the id argument uses an autoincrementing id.")
        } else {
            // For String IDs, keep the existing behavior (required parameter)
            Method::new(
                "constructor",
                vec![Param::new("id", NamedType::new(id_kind.type_name()))],
                None,
                format!(
                    r#"
      super()
      this.set('id', {})"#,
                    id_kind.value_from()
                ),
            )
        }
    }

    fn generate_store_methods(&self, entity_name: &str, id_kind: &IdFieldKind) -> Vec<StoreMethod> {
        // Generate save() method - different for auto-increment vs string IDs
        let save_method = if id_kind.supports_auto() {
            // For Int8 and Bytes, check if id is null/unset and use "auto" as the key
            Method::new(
                "save",
                vec![],
                Some(NamedType::new("void").into()),
                format!(
                    r#"
        let id = this.get('id')
        if (id == null || id.kind == ValueKind.NULL) {{
          store.set('{}', 'auto', this)
        }} else {{
          assert(id.kind == {},
                 `Entities of type {} must have an ID of type {} but the id '${{id.displayData()}}' is of type ${{id.displayKind()}}`)
          store.set('{}', {}, this)
        }}"#,
                    entity_name,
                    id_kind.value_kind(),
                    entity_name,
                    id_kind.gql_type_name(),
                    entity_name,
                    id_kind.value_to_string()
                ),
            )
        } else {
            // For String IDs, keep the existing behavior (require ID)
            Method::new(
                "save",
                vec![],
                Some(NamedType::new("void").into()),
                format!(
                    r#"
        let id = this.get('id')
        assert(id != null,
               'Cannot save {} entity without an ID')
        if (id) {{
          assert(id.kind == {},
                 `Entities of type {} must have an ID of type {} but the id '${{id.displayData()}}' is of type ${{id.displayKind()}}`)
          store.set('{}', {}, this)
        }}"#,
                    entity_name,
                    id_kind.value_kind(),
                    entity_name,
                    id_kind.gql_type_name(),
                    entity_name,
                    id_kind.value_to_string()
                ),
            )
        };

        vec![
            StoreMethod::Regular(save_method),
            // loadInBlock() static method
            StoreMethod::Static(StaticMethod::new(
                "loadInBlock",
                vec![Param::new("id", NamedType::new(id_kind.type_name()))],
                NullableType::new(NamedType::new(entity_name)),
                format!(
                    r#"
        return changetype<{} | null>(store.get_in_block('{}', {}))"#,
                    entity_name,
                    entity_name,
                    id_kind.id_to_string_code()
                ),
            )),
            // load() static method
            StoreMethod::Static(StaticMethod::new(
                "load",
                vec![Param::new("id", NamedType::new(id_kind.type_name()))],
                NullableType::new(NamedType::new(entity_name)),
                format!(
                    r#"
        return changetype<{} | null>(store.get('{}', {}))"#,
                    entity_name,
                    entity_name,
                    id_kind.id_to_string_code()
                ),
            )),
        ]
    }

    fn generate_field_getter(
        &self,
        entity_name: &str,
        field: &FieldInfo,
        id_kind: &IdFieldKind,
    ) -> Option<Method> {
        let safe_name = handle_reserved_word(&field.name);

        // Handle derived fields
        if field.is_derived {
            return self.generate_derived_field_getter(entity_name, field, &safe_name, id_kind);
        }

        let value_type = self.value_type_from_field(field);
        let return_type = self.type_from_field(field);
        let nullable = field.is_nullable;

        let primitive_default = match &return_type {
            TypeExpr::Named(t) => t.get_primitive_default(),
            _ => None,
        };

        let get_code = if nullable {
            format!(
                r#"
       let value = this.get('{}')
       if (!value || value.kind == ValueKind.NULL) {{
         return null
       }} else {{
         return {}
       }}"#,
                field.name,
                value_to_asc("value", &value_type)
            )
        } else {
            let null_handling = match primitive_default {
                Some(default) => format!("return {}", default),
                None => "throw new Error('Cannot return null for a required field.')".to_string(),
            };
            format!(
                r#"
       let value = this.get('{}')
       if (!value || value.kind == ValueKind.NULL) {{
         {}
       }} else {{
         return {}
       }}"#,
                field.name,
                null_handling,
                value_to_asc("value", &value_type)
            )
        };

        Some(Method::new(
            format!("get {}", safe_name),
            vec![],
            Some(return_type),
            get_code,
        ))
    }

    fn generate_derived_field_getter(
        &self,
        entity_name: &str,
        field: &FieldInfo,
        safe_name: &str,
        id_kind: &IdFieldKind,
    ) -> Option<Method> {
        let loader_name = format!("{}Loader", field.base_type);

        let id_conversion = match id_kind {
            IdFieldKind::Bytes => "this.get('id')!.toBytes().toHexString()",
            _ => "this.get('id')!.toString()",
        };

        Some(Method::new(
            format!("get {}", safe_name),
            vec![],
            Some(NamedType::new(&loader_name).into()),
            format!(
                r#"
        return new {}('{}', {}, '{}')"#,
                loader_name, entity_name, id_conversion, field.name
            ),
        ))
    }

    fn generate_field_setter(&self, field: &FieldInfo) -> Option<Method> {
        // No setters for derived fields
        if field.is_derived {
            return None;
        }

        let safe_name = handle_reserved_word(&field.name);
        let value_type = self.value_type_from_field(field);
        let param_type = self.type_from_field(field);
        let nullable = field.is_nullable;

        let set_code = if nullable {
            let inner_type = match &param_type {
                TypeExpr::Nullable(n) => n.inner.to_string(),
                other => other.to_string(),
            };
            format!(
                r#"
      if (!value) {{
        this.unset('{}')
      }} else {{
        this.set('{}', {})
      }}"#,
                field.name,
                field.name,
                value_from_asc(&format!("<{}>value", inner_type), &value_type)
            )
        } else {
            format!(
                r#"
      this.set('{}', {})"#,
                field.name,
                value_from_asc("value", &value_type)
            )
        };

        Some(Method::new(
            format!("set {}", safe_name),
            vec![Param::new("value", param_type)],
            None,
            set_code,
        ))
    }

    fn generate_derived_loader(&self, type_name: &str) -> Class {
        let loader_name = format!("{}Loader", type_name);
        let mut klass = ts::klass(&loader_name).exported().extends("Entity");

        // Add members
        klass.add_member(ts::klass_member("_entity", "string"));
        klass.add_member(ts::klass_member("_field", "string"));
        klass.add_member(ts::klass_member("_id", "string"));

        // Add constructor
        klass.add_method(Method::new(
            "constructor",
            vec![
                Param::new("entity", NamedType::new("string")),
                Param::new("id", NamedType::new("string")),
                Param::new("field", NamedType::new("string")),
            ],
            None,
            r#"
      super();
      this._entity = entity;
      this._id = id;
      this._field = field;"#
                .to_string(),
        ));

        // Add load() method
        klass.add_method(Method::new(
            "load",
            vec![],
            Some(TypeExpr::Raw(format!("{}[]", type_name))),
            format!(
                r#"
  let value = store.loadRelated(this._entity, this._id, this._field);
  return changetype<{}[]>(value);"#,
                type_name
            ),
        ));

        klass
    }

    /// Get the value type string for a field.
    ///
    /// Returns the GraphQL-style value type string:
    /// - Scalars: `String`, `Int`, `BigInt`, etc.
    /// - Arrays: `[String]`, `[Int]`, etc.
    /// - Nested arrays: `[[String]]`, `[[Int]]`, etc.
    /// - Entity references are converted to the referenced entity's ID type
    fn value_type_from_field(&self, field: &FieldInfo) -> String {
        let base = if let Some(id_kind) = self.entity_id_kinds.get(&field.base_type) {
            id_kind.gql_type_name().to_string()
        } else {
            field.base_type.clone()
        };

        // Wrap with brackets for each level of list nesting
        let mut result = base;
        for _ in 0..field.list_depth {
            result = format!("[{}]", result);
        }
        result
    }

    /// Convert field info to an AssemblyScript TypeExpr.
    ///
    /// Creates the correct type expression including nested arrays:
    /// - Scalars: `string`, `i32`, `BigInt`, etc.
    /// - Arrays: `Array<string>`, `Array<i32>`, etc.
    /// - Nested arrays: `Array<Array<string>>`, etc.
    fn type_from_field(&self, field: &FieldInfo) -> TypeExpr {
        let type_name = if let Some(id_kind) = self.entity_id_kinds.get(&field.base_type) {
            id_kind.type_name()
        } else {
            asc_type_for_value(&field.base_type)
        };

        let named = NamedType::new(type_name);

        if field.list_depth > 0 {
            // Use ArrayType::with_depth to create nested array types
            let array_type = ArrayType::with_depth(named, field.list_depth);
            if field.is_nullable {
                NullableType::new(array_type).into()
            } else {
                array_type
            }
        } else if field.is_nullable && !named.is_primitive() {
            NullableType::new(named).into()
        } else {
            named.into()
        }
    }
}

enum StoreMethod {
    Regular(Method),
    Static(StaticMethod),
}

#[cfg(test)]
mod tests {
    use super::*;
    use graphql_tools::parser::parse_schema;

    #[test]
    fn test_simple_entity() {
        let schema = r#"
            type Transfer @entity {
                id: ID!
                from: Bytes!
                to: Bytes!
                value: BigInt!
            }
        "#;
        let doc = parse_schema::<String>(schema).unwrap();
        let gen = SchemaCodeGenerator::new(&doc).unwrap();

        let classes = gen.generate_types(true);
        assert_eq!(classes.len(), 1);

        let transfer = &classes[0];
        assert_eq!(transfer.name, "Transfer");
        assert_eq!(transfer.extends, Some("Entity".to_string()));
        assert!(transfer.export);
    }

    #[test]
    fn test_nullable_field() {
        let schema = r#"
            type Token @entity {
                id: ID!
                name: String
                symbol: String!
            }
        "#;
        let doc = parse_schema::<String>(schema).unwrap();
        let gen = SchemaCodeGenerator::new(&doc).unwrap();

        let classes = gen.generate_types(true);
        assert_eq!(classes.len(), 1);

        // Check that we have methods for nullable and non-nullable fields
        let token = &classes[0];
        let method_names: Vec<_> = token.methods.iter().map(|m| m.name.as_str()).collect();
        assert!(method_names.contains(&"get name"));
        assert!(method_names.contains(&"set name"));
        assert!(method_names.contains(&"get symbol"));
        assert!(method_names.contains(&"set symbol"));
    }

    #[test]
    fn test_id_field_types() {
        assert_eq!(IdFieldKind::String.type_name(), "string");
        assert_eq!(IdFieldKind::Bytes.type_name(), "Bytes");
        assert_eq!(IdFieldKind::Int8.type_name(), "i64");
    }

    #[test]
    fn test_auto_increment_support() {
        // String IDs don't support auto-increment
        assert!(!IdFieldKind::String.supports_auto());
        assert!(IdFieldKind::String.constructor_default().is_none());

        // Int8 IDs support auto-increment
        assert!(IdFieldKind::Int8.supports_auto());
        assert_eq!(IdFieldKind::Int8.constructor_param_type(), "i64");
        assert_eq!(
            IdFieldKind::Int8.constructor_default(),
            Some("i64.MIN_VALUE")
        );
        assert_eq!(
            IdFieldKind::Int8.auto_check_condition(),
            "id != i64.MIN_VALUE"
        );

        // Bytes IDs support auto-increment
        assert!(IdFieldKind::Bytes.supports_auto());
        assert_eq!(IdFieldKind::Bytes.constructor_param_type(), "Bytes | null");
        assert_eq!(IdFieldKind::Bytes.constructor_default(), Some("null"));
        assert_eq!(IdFieldKind::Bytes.auto_check_condition(), "id");
    }

    #[test]
    fn test_int8_id_auto_increment_codegen() {
        let schema = r#"
            type Counter @entity {
                id: Int8!
                value: BigInt!
            }
        "#;
        let doc = parse_schema::<String>(schema).unwrap();
        let gen = SchemaCodeGenerator::new(&doc).unwrap();

        let classes = gen.generate_types(true);
        assert_eq!(classes.len(), 1);

        let counter = &classes[0];
        let output = counter.to_string();

        // Constructor should have optional id with sentinel default
        assert!(
            output.contains("constructor(id: i64 = i64.MIN_VALUE)"),
            "Int8 ID constructor should have default sentinel value, got: {}",
            output
        );

        // Constructor should have doc comment
        assert!(
            output.contains("Leaving out the id argument uses an autoincrementing id"),
            "Constructor should have auto-increment doc comment"
        );

        // Constructor body should conditionally set id
        assert!(
            output.contains("if (id != i64.MIN_VALUE)"),
            "Constructor should check for sentinel value"
        );

        // save() method should use "auto" when id is null
        assert!(
            output.contains("store.set('Counter', 'auto', this)"),
            "save() should use 'auto' key for auto-increment, got: {}",
            output
        );
    }

    #[test]
    fn test_bytes_id_auto_increment_codegen() {
        let schema = r#"
            type Event @entity {
                id: Bytes!
                data: String!
            }
        "#;
        let doc = parse_schema::<String>(schema).unwrap();
        let gen = SchemaCodeGenerator::new(&doc).unwrap();

        let classes = gen.generate_types(true);
        assert_eq!(classes.len(), 1);

        let event = &classes[0];
        let output = event.to_string();

        // Constructor should have nullable id with null default
        assert!(
            output.contains("constructor(id: Bytes | null = null)"),
            "Bytes ID constructor should have null default, got: {}",
            output
        );

        // Constructor should have doc comment
        assert!(
            output.contains("Leaving out the id argument uses an autoincrementing id"),
            "Constructor should have auto-increment doc comment"
        );

        // Constructor body should conditionally set id
        assert!(
            output.contains("if (id)"),
            "Constructor should check for null"
        );

        // save() method should use "auto" when id is null
        assert!(
            output.contains("store.set('Event', 'auto', this)"),
            "save() should use 'auto' key for auto-increment, got: {}",
            output
        );
    }

    #[test]
    fn test_string_id_no_auto_increment() {
        let schema = r#"
            type User @entity {
                id: ID!
                name: String!
            }
        "#;
        let doc = parse_schema::<String>(schema).unwrap();
        let gen = SchemaCodeGenerator::new(&doc).unwrap();

        let classes = gen.generate_types(true);
        assert_eq!(classes.len(), 1);

        let user = &classes[0];
        let output = user.to_string();

        // Constructor should have required id (no default)
        assert!(
            output.contains("constructor(id: string)"),
            "String ID constructor should have required parameter, got: {}",
            output
        );

        // Constructor should NOT have auto-increment doc comment
        assert!(
            !output.contains("autoincrementing"),
            "String ID should not mention auto-increment"
        );

        // save() method should NOT use "auto"
        assert!(
            !output.contains("'auto'"),
            "String ID save() should not use 'auto' key"
        );

        // save() should require ID
        assert!(
            output.contains("Cannot save User entity without an ID"),
            "String ID save() should require ID"
        );
    }

    #[test]
    fn test_entity_reference() {
        let schema = r#"
            type User @entity {
                id: ID!
                name: String!
            }
            type Post @entity {
                id: ID!
                author: User!
            }
        "#;
        let doc = parse_schema::<String>(schema).unwrap();
        let gen = SchemaCodeGenerator::new(&doc).unwrap();

        // The Post.author field should be treated as a string (entity ID reference)
        assert!(gen.entity_names.contains("User"));
        assert!(gen.entity_names.contains("Post"));
    }

    #[test]
    fn test_simple_array_field() {
        let schema = r#"
            type Token @entity {
                id: ID!
                holders: [String!]!
            }
        "#;
        let doc = parse_schema::<String>(schema).unwrap();
        let gen = SchemaCodeGenerator::new(&doc).unwrap();

        let classes = gen.generate_types(true);
        assert_eq!(classes.len(), 1);

        let token = &classes[0];
        let output = token.to_string();

        // Verify array field getter/setter are generated
        assert!(
            output.contains("get holders()"),
            "Should have holders getter"
        );
        assert!(
            output.contains("set holders("),
            "Should have holders setter"
        );

        // Check the type is Array<string>
        assert!(
            output.contains("Array<string>"),
            "Array field should use Array<string> type"
        );
    }

    #[test]
    fn test_nested_array_field() {
        let schema = r#"
            type Matrix @entity {
                id: ID!
                stringMatrix: [[String!]!]!
                intMatrix: [[Int!]!]
                bigIntMatrix: [[BigInt!]!]!
            }
        "#;
        let doc = parse_schema::<String>(schema).unwrap();
        let gen = SchemaCodeGenerator::new(&doc).unwrap();

        let classes = gen.generate_types(true);
        assert_eq!(classes.len(), 1);

        let matrix = &classes[0];
        let output = matrix.to_string();

        // Verify nested array field getter/setter are generated
        assert!(
            output.contains("get stringMatrix()"),
            "Should have stringMatrix getter"
        );
        assert!(
            output.contains("set stringMatrix("),
            "Should have stringMatrix setter"
        );

        // Check the type is Array<Array<string>>
        assert!(
            output.contains("Array<Array<string>>"),
            "Nested array field should use Array<Array<string>> type, got: {}",
            output
        );

        // Check that toStringMatrix() and fromStringMatrix() are used
        assert!(
            output.contains("toStringMatrix()"),
            "Should use toStringMatrix() for nested string arrays"
        );
        assert!(
            output.contains("fromStringMatrix("),
            "Should use Value.fromStringMatrix() for nested string arrays"
        );

        // Check BigInt matrix uses correct methods
        assert!(
            output.contains("Array<Array<BigInt>>"),
            "BigInt matrix should use Array<Array<BigInt>> type"
        );
        assert!(
            output.contains("toBigIntMatrix()"),
            "Should use toBigIntMatrix() for nested BigInt arrays"
        );
        assert!(
            output.contains("fromBigIntMatrix("),
            "Should use Value.fromBigIntMatrix() for nested BigInt arrays"
        );

        // Check nullable nested array has correct type
        assert!(
            output.contains("Array<Array<i32>> | null"),
            "Nullable nested array should be Array<Array<i32>> | null"
        );
    }

    #[test]
    fn test_list_depth() {
        use graphql_tools::parser::parse_schema;

        // Helper to get list depth from schema field type
        fn get_field_list_depth(schema_str: &str) -> u8 {
            let doc = parse_schema::<String>(schema_str).unwrap();
            for def in &doc.definitions {
                if let Definition::TypeDefinition(TypeDefinition::Object(obj)) = def {
                    for field in &obj.fields {
                        if field.name == "field" {
                            return list_depth(&field.field_type);
                        }
                    }
                }
            }
            panic!("Field not found");
        }

        // Scalar
        assert_eq!(
            get_field_list_depth("type T @entity { id: ID!, field: String! }"),
            0
        );

        // Simple array
        assert_eq!(
            get_field_list_depth("type T @entity { id: ID!, field: [String!]! }"),
            1
        );

        // Nested array (matrix)
        assert_eq!(
            get_field_list_depth("type T @entity { id: ID!, field: [[String!]!]! }"),
            2
        );

        // Triple nested array
        assert_eq!(
            get_field_list_depth("type T @entity { id: ID!, field: [[[String!]!]!]! }"),
            3
        );
    }

    #[test]
    fn test_value_type_from_field_nested() {
        let schema = r#"
            type Test @entity {
                id: ID!
                scalar: String!
                array: [String!]!
                matrix: [[String!]!]!
            }
        "#;
        let doc = parse_schema::<String>(schema).unwrap();
        let gen = SchemaCodeGenerator::new(&doc).unwrap();

        // Find the entity
        let entity = &gen.entities[0];

        // Find each field and check its value type
        let scalar_field = entity.fields.iter().find(|f| f.name == "scalar").unwrap();
        let array_field = entity.fields.iter().find(|f| f.name == "array").unwrap();
        let matrix_field = entity.fields.iter().find(|f| f.name == "matrix").unwrap();

        assert_eq!(gen.value_type_from_field(scalar_field), "String");
        assert_eq!(gen.value_type_from_field(array_field), "[String]");
        assert_eq!(gen.value_type_from_field(matrix_field), "[[String]]");
    }

    #[test]
    fn test_bytes_id_entity_reference() {
        let schema = r#"
            type Token @entity {
                id: Bytes!
                name: String!
            }
            type Balance @entity {
                id: ID!
                token: Token!
                amount: BigInt!
            }
        "#;
        let doc = parse_schema::<String>(schema).unwrap();
        let gen = SchemaCodeGenerator::new(&doc).unwrap();

        let classes = gen.generate_types(true);
        let balance = classes.iter().find(|c| c.name == "Balance").unwrap();
        let output = balance.to_string();

        // Getter should return Bytes and use toBytes()
        assert!(
            output.contains("value.toBytes()"),
            "Bytes-ID entity reference getter should use toBytes(), got: {}",
            output
        );

        // Setter should use Value.fromBytes()
        assert!(
            output.contains("Value.fromBytes("),
            "Bytes-ID entity reference setter should use Value.fromBytes(), got: {}",
            output
        );

        // Return type should be Bytes, not string
        assert!(
            output.contains("get token(): Bytes"),
            "Bytes-ID entity reference getter should return Bytes, got: {}",
            output
        );
    }

    #[test]
    fn test_int8_id_entity_reference() {
        let schema = r#"
            type Counter @entity {
                id: Int8!
                value: BigInt!
            }
            type Snapshot @entity {
                id: ID!
                counter: Counter!
            }
        "#;
        let doc = parse_schema::<String>(schema).unwrap();
        let gen = SchemaCodeGenerator::new(&doc).unwrap();

        let classes = gen.generate_types(true);
        let snapshot = classes.iter().find(|c| c.name == "Snapshot").unwrap();
        let output = snapshot.to_string();

        // Getter should return i64 and use toI64()
        assert!(
            output.contains("value.toI64()"),
            "Int8-ID entity reference getter should use toI64(), got: {}",
            output
        );

        // Setter should use Value.fromI64()
        assert!(
            output.contains("Value.fromI64("),
            "Int8-ID entity reference setter should use Value.fromI64(), got: {}",
            output
        );

        // Return type should be i64
        assert!(
            output.contains("get counter(): i64"),
            "Int8-ID entity reference getter should return i64, got: {}",
            output
        );
    }

    #[test]
    fn test_mixed_id_entity_references() {
        let schema = r#"
            type User @entity {
                id: ID!
                name: String!
            }
            type Token @entity {
                id: Bytes!
                owner: User!
            }
        "#;
        let doc = parse_schema::<String>(schema).unwrap();
        let gen = SchemaCodeGenerator::new(&doc).unwrap();

        let classes = gen.generate_types(true);
        let token = classes.iter().find(|c| c.name == "Token").unwrap();
        let output = token.to_string();

        // Token.owner references User which has String ID
        assert!(
            output.contains("get owner(): string"),
            "Reference to String-ID entity should use string type, got: {}",
            output
        );
        assert!(
            output.contains("value.toString()"),
            "Reference to String-ID entity should use toString(), got: {}",
            output
        );
    }

    #[test]
    fn test_nullable_bytes_id_entity_reference() {
        let schema = r#"
            type Token @entity {
                id: Bytes!
                name: String!
            }
            type Balance @entity {
                id: ID!
                token: Token
                amount: BigInt!
            }
        "#;
        let doc = parse_schema::<String>(schema).unwrap();
        let gen = SchemaCodeGenerator::new(&doc).unwrap();

        let classes = gen.generate_types(true);
        let balance = classes.iter().find(|c| c.name == "Balance").unwrap();
        let output = balance.to_string();

        // Nullable Bytes reference should be `Bytes | null`
        assert!(
            output.contains("get token(): Bytes | null"),
            "Nullable Bytes-ID reference should return Bytes | null, got: {}",
            output
        );
    }

    #[test]
    fn test_derived_field_with_bytes_id_parent() {
        let schema = r#"
            type Token @entity {
                id: Bytes!
                balances: [Balance!]! @derivedFrom(field: "token")
            }
            type Balance @entity {
                id: ID!
                token: Token!
                amount: BigInt!
            }
        "#;
        let doc = parse_schema::<String>(schema).unwrap();
        let gen = SchemaCodeGenerator::new(&doc).unwrap();

        let classes = gen.generate_types(true);
        let token = classes.iter().find(|c| c.name == "Token").unwrap();
        let output = token.to_string();

        // Derived field getter on Bytes-ID entity should use toBytes().toHexString()
        assert!(
            output.contains("this.get('id')!.toBytes().toHexString()"),
            "Derived field on Bytes-ID entity should use toBytes().toHexString(), got: {}",
            output
        );
    }
}
