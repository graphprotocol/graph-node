use std::fmt;

use anyhow::{bail, Context, Result};

use crate::{amp::common::Ident, cheap_clone::CheapClone, data::store::ValueType};

/// A minimal representation of a subgraph entity.
pub(super) struct Entity {
    name: Ident,
    fields: Vec<Field>,
}

impl Entity {
    /// Converts the Arrow schema to a subgraph entity.
    ///
    /// # Errors
    ///
    /// Returns an error if Arrow fields cannot be converted to subgraph entity fields.
    ///
    /// The returned error is deterministic.
    pub(super) fn new(name: Ident, arrow_schema: arrow::datatypes::Schema) -> Result<Self> {
        let mut fields = arrow_schema
            .fields()
            .iter()
            .map(|field| {
                Field::new(field)
                    .with_context(|| format!("failed to create field '{}'", field.name()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        if !fields.iter().any(|field| field.name.as_str() == "id") {
            fields.push(Field::id());
        }

        fields.sort_unstable_by_key(|field| field.name.cheap_clone());

        Ok(Self { name, fields })
    }
}

impl fmt::Display for Entity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write! {f, "type {} @entity(immutable: true)", self.name.to_upper_camel_case()}?;
        write! {f, " {{\n"}?;
        for field in &self.fields {
            write! {f, "\t{field}\n"}?;
        }
        write! {f, "}}"}
    }
}

/// A minimal representation of a subgraph entity field.
struct Field {
    name: Ident,
    value_type: ValueType,
    is_list: bool,
    is_required: bool,
}

impl Field {
    /// Converts the Arrow field to a subgraph entity field.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The Arrow field has an invalid name
    /// - The Arrow field type cannot be converted to a subgraph entity value type
    ///
    /// The returned error is deterministic.
    fn new(arrow_field: &arrow::datatypes::Field) -> Result<Self> {
        let name = Ident::new(arrow_field.name())?;
        let (value_type, is_list) = arrow_data_type_to_value_type(arrow_field.data_type())?;
        let is_required = !arrow_field.is_nullable();

        Ok(Self {
            name,
            value_type,
            is_list,
            is_required,
        })
    }

    /// Creates an `ID` subgraph entity field.
    fn id() -> Self {
        Self {
            name: Ident::new("id").unwrap(),
            value_type: ValueType::Bytes,
            is_list: false,
            is_required: true,
        }
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write! {f, "{}: ", self.name.to_lower_camel_case()}?;
        if self.is_list {
            write! {f, "["}?;
        }
        write! {f, "{}", self.value_type.to_str()}?;
        if self.is_list {
            write! {f, "]"}?;
        }
        if self.is_required {
            write! {f, "!"}?;
        }
        Ok(())
    }
}

fn arrow_data_type_to_value_type(
    data_type: &arrow::datatypes::DataType,
) -> Result<(ValueType, bool)> {
    use arrow::datatypes::DataType::*;

    let type_not_supported = || bail!("type '{data_type}' not supported");
    let value_type = match data_type {
        Null => return type_not_supported(),
        Boolean => ValueType::Boolean,
        Int8 => ValueType::Int,
        Int16 => ValueType::Int,
        Int32 => ValueType::Int,
        Int64 => ValueType::Int8,
        UInt8 => ValueType::Int,
        UInt16 => ValueType::Int,
        UInt32 => ValueType::Int8,
        UInt64 => ValueType::BigInt,
        Float16 => ValueType::BigDecimal,
        Float32 => ValueType::BigDecimal,
        Float64 => ValueType::BigDecimal,
        Timestamp(_, _) => ValueType::Timestamp,
        Date32 => ValueType::Timestamp,
        Date64 => ValueType::Timestamp,
        Time32(_) => return type_not_supported(),
        Time64(_) => return type_not_supported(),
        Duration(_) => return type_not_supported(),
        Interval(_) => return type_not_supported(),
        Binary => ValueType::Bytes,
        FixedSizeBinary(_) => ValueType::Bytes,
        LargeBinary => ValueType::Bytes,
        BinaryView => ValueType::Bytes,
        Utf8 => ValueType::String,
        LargeUtf8 => ValueType::String,
        Utf8View => ValueType::String,
        List(field)
        | ListView(field)
        | FixedSizeList(field, _)
        | LargeList(field)
        | LargeListView(field) => {
            if field.data_type().is_nested() {
                return type_not_supported();
            }

            return arrow_data_type_to_value_type(field.data_type())
                .map(|(value_type, _)| (value_type, true));
        }
        Struct(_) => return type_not_supported(),
        Union(_, _) => return type_not_supported(),
        Dictionary(_, _) => return type_not_supported(),
        Decimal128(_, _) => ValueType::BigDecimal,
        Decimal256(_, _) => ValueType::BigDecimal,
        Map(_, _) => return type_not_supported(),
        RunEndEncoded(_, _) => return type_not_supported(),
    };

    Ok((value_type, false))
}
