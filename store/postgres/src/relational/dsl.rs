//! Helpers for creating relational queries using diesel. A lot of this code
//! is copied from `diesel_dynamic_schema` and adapted to our data
//! structures, especially the `Table` and `Column` types.

use std::marker::PhantomData;

use diesel::backend::Backend;
use diesel::dsl::sql;
use diesel::expression::{expression_types, is_aggregate, TypedExpressionType, ValidGrouping};
use diesel::pg::Pg;
use diesel::query_builder::{
    AsQuery, AstPass, BoxedSelectStatement, FromClause, Query, QueryFragment, QueryId,
    SelectStatement,
};
use diesel::query_dsl::methods::SelectDsl;
use diesel::query_source::QuerySource;

use diesel::sql_types::{
    Array, BigInt, Binary, Bool, Integer, Nullable, Numeric, SingleValue, Text, Timestamptz,
    Untyped,
};
use diesel::{AppearsOnTable, Expression, QueryDsl, QueryResult, SelectableExpression};
use diesel_dynamic_schema::DynamicSelectClause;
use graph::components::store::{AttributeNames, BlockNumber, StoreError, BLOCK_NUMBER_MAX};
use graph::data::store::{Id, IdType, ID};
use graph::data_source::CausalityRegion;
use graph::prelude::{lazy_static, ENV_VARS};

use crate::relational::ColumnType;
use crate::relational_queries::PARENT_ID;

use super::value::FromOidRow;
use super::Column as RelColumn;
use super::SqlName;
use super::{BLOCK_COLUMN, BLOCK_RANGE_COLUMN};

const TYPENAME: &str = "__typename";

lazy_static! {
    pub static ref TYPENAME_SQL: SqlName = TYPENAME.into();
    pub static ref VID_SQL: SqlName = "vid".into();
    pub static ref PARENT_SQL: SqlName = PARENT_ID.into();
    pub static ref TYPENAME_COL: RelColumn = RelColumn::pseudo_column(TYPENAME, ColumnType::String);
    pub static ref VID_COL: RelColumn = RelColumn::pseudo_column("vid", ColumnType::Int8);
    pub static ref BLOCK_COL: RelColumn = RelColumn::pseudo_column(BLOCK_COLUMN, ColumnType::Int8);
    // The column type is a placeholder, we can't deserialize in4range; but
    // we also never try to use it when we get data from the database
    pub static ref BLOCK_RANGE_COL: RelColumn =
        RelColumn::pseudo_column(BLOCK_RANGE_COLUMN, ColumnType::Bytes);
    pub static ref PARENT_STRING_COL: RelColumn = RelColumn::pseudo_column(PARENT_ID, ColumnType::String);
    pub static ref PARENT_BYTES_COL: RelColumn = RelColumn::pseudo_column(PARENT_ID, ColumnType::Bytes);
    pub static ref PARENT_INT_COL: RelColumn = RelColumn::pseudo_column(PARENT_ID, ColumnType::Int8);

    pub static ref META_COLS: [&'static RelColumn; 2] = [&*TYPENAME_COL, &*VID_COL];
}

#[doc(hidden)]
/// A dummy expression.
pub struct DummyExpression;

impl DummyExpression {
    pub(crate) fn new() -> Self {
        DummyExpression
    }
}

impl<QS> SelectableExpression<QS> for DummyExpression {}

impl<QS> AppearsOnTable<QS> for DummyExpression {}

impl Expression for DummyExpression {
    type SqlType = expression_types::NotSelectable;
}

impl ValidGrouping<()> for DummyExpression {
    type IsAggregate = is_aggregate::No;
}

/// A fixed size string for the table alias. We want to make sure that
/// converting these to `&str` doesn't allocate and that they are small
/// enough that the `Table` struct is only 16 bytes and can be `Copy`
#[derive(Debug, Clone, Copy)]
pub struct ChildAliasStr {
    alias: [u8; 4],
}

impl ChildAliasStr {
    fn new(idx: u8) -> Self {
        let c = 'i' as u8;
        let alias = if idx == 0 {
            [c, 0, 0, 0]
        } else if idx < 10 {
            let ones = char::from_digit(idx as u32, 10).unwrap() as u8;
            [c, ones, 0, 0]
        } else if idx < 100 {
            let tens = char::from_digit((idx / 10) as u32, 10).unwrap() as u8;
            let ones = char::from_digit((idx % 10) as u32, 10).unwrap() as u8;
            [c, tens, ones, 0]
        } else {
            let hundreds = char::from_digit((idx / 100) as u32, 10).unwrap() as u8;
            let idx = idx % 100;
            let tens = char::from_digit((idx / 10) as u32, 10).unwrap() as u8;
            let ones = char::from_digit((idx % 10) as u32, 10).unwrap() as u8;
            [c, hundreds, tens, ones]
        };
        ChildAliasStr { alias }
    }

    fn as_str(&self) -> &str {
        let alias = if self.alias[1] == 0 {
            return "i";
        } else if self.alias[2] == 0 {
            &self.alias[..2]
        } else if self.alias[3] == 0 {
            &self.alias[..3]
        } else {
            &self.alias
        };
        unsafe { std::str::from_utf8_unchecked(alias) }
    }
}

/// A table alias. We use `c` as the main table alias and `i`, `i1`, `i2`,
/// ... for child tables. The fact that we use these specific letters is
/// historical and doesn't have any meaning.
#[derive(Debug, Clone, Copy)]
pub enum Alias {
    Main,
    Child(ChildAliasStr),
}

impl Alias {
    fn as_str(&self) -> &str {
        match self {
            Alias::Main => "c",
            Alias::Child(idx) => idx.as_str(),
        }
    }

    fn child(idx: u8) -> Self {
        Alias::Child(ChildAliasStr::new(idx))
    }
}

#[test]
fn alias() {
    assert_eq!(Alias::Main.as_str(), "c");
    assert_eq!(Alias::Child(ChildAliasStr::new(0)).as_str(), "i");
    assert_eq!(Alias::Child(ChildAliasStr::new(1)).as_str(), "i1");
    assert_eq!(Alias::Child(ChildAliasStr::new(10)).as_str(), "i10");
    assert_eq!(Alias::Child(ChildAliasStr::new(100)).as_str(), "i100");
    assert_eq!(Alias::Child(ChildAliasStr::new(255)).as_str(), "i255");
}

#[derive(Debug, Clone, Copy)]
/// A wrapper around the `super::Table` struct that provides helper
/// functions for generating SQL queries
pub struct Table<'a> {
    /// The metadata for this table
    pub meta: &'a super::Table,
    alias: Alias,
}

impl<'a> Table<'a> {
    pub(crate) fn new(meta: &'a super::Table) -> Self {
        Self {
            meta,
            alias: Alias::Main,
        }
    }

    /// Change the alias for this table to be a child table.
    pub fn child(mut self, idx: u8) -> Self {
        self.alias = Alias::child(idx);
        self
    }

    /// Reference a column in this table and use the correct SQL type `ST`
    fn bind<ST>(&self, name: &str) -> Option<BoundColumn<ST>> {
        self.column(name).map(|c| c.bind())
    }

    /// Reference a column without regard to the underlying SQL type. This
    /// is useful if just the name of the column qualified with the table
    /// name/alias is needed
    pub fn column(&self, name: &str) -> Option<Column<'a>> {
        self.meta
            .columns
            .iter()
            .chain(META_COLS.into_iter())
            .find(|c| &c.name == name)
            .map(|c| Column::new(self.clone(), c))
    }

    pub fn name(&self) -> &str {
        &self.meta.name
    }

    pub fn column_for_field(&self, field: &str) -> Result<Column<'a>, StoreError> {
        self.meta
            .column_for_field(field)
            .map(|column| Column::new(*self, column))
    }

    pub fn primary_key(&self) -> Column<'a> {
        Column::new(*self, self.meta.primary_key())
    }

    /// Return a filter expression that generates the SQL for `id = $id`
    pub fn id_eq(&'a self, id: &'a Id) -> IdEq<'a> {
        IdEq::new(*self, id)
    }

    /// Return an expression that generates the SQL for `block_range @>
    /// $block` or `block = $block` depending on whether the table is
    /// mutable or not
    pub fn at_block(&self, block: BlockNumber) -> AtBlock<'a> {
        AtBlock::new(*self, block)
    }

    /// The block column for this table for places where the just the
    /// qualified name is needed
    pub fn block_column(&self) -> BlockColumn<'a> {
        BlockColumn::new(*self)
    }

    /// An expression that is true if the entity has changed since `block`
    pub fn changed_since(&self, block: BlockNumber) -> ChangedSince<'a> {
        let column = self.block_column();
        ChangedSince { column, block }
    }

    /// Return an expression that generates the SQL for `causality_region =
    /// $cr` if the table uses causality regions
    pub fn belongs_to_causality_region(
        &'a self,
        cr: CausalityRegion,
    ) -> BelongsToCausalityRegion<'a> {
        BelongsToCausalityRegion::new(*self, cr)
    }

    /// Produce a list of the columns that should be selected for a query
    /// based on `column_names`. The result needs to be used both to create
    /// the actual select statement with `Self::select_cols` and to decode
    /// query results with `FromOidRow`.
    pub fn selected_columns<T: FromOidRow>(
        &self,
        column_names: &'a AttributeNames,
        parent_type: Option<IdType>,
    ) -> Result<Vec<&'a super::Column>, StoreError> {
        let mut cols = Vec::new();
        if T::WITH_INTERNAL_KEYS {
            cols.push(&*TYPENAME_COL);
        }

        match column_names {
            AttributeNames::All => cols.extend(self.meta.columns.iter()),
            AttributeNames::Select(names) => {
                let pk = self.meta.primary_key();
                cols.push(pk);
                let mut names: Vec<_> = names.iter().filter(|name| *name != &*ID).collect();
                names.sort();
                for name in names {
                    let column = self.meta.column_for_field(&name)?;
                    cols.push(column);
                }
            }
        };

        // NB: Exclude full-text search columns from selection. These columns are used for indexing
        // and searching but are not part of the entity's data model.
        cols.retain(|c| !c.is_fulltext());

        if T::WITH_INTERNAL_KEYS {
            match parent_type {
                Some(IdType::String) => cols.push(&*PARENT_STRING_COL),
                Some(IdType::Bytes) => cols.push(&*PARENT_BYTES_COL),
                Some(IdType::Int8) => cols.push(&*PARENT_INT_COL),
                None => (),
            }
        }

        if T::WITH_SYSTEM_COLUMNS {
            cols.push(&*VID_COL);
            if self.meta.immutable {
                cols.push(&*BLOCK_COL);
            } else {
                // TODO: We can't deserialize in4range
                cols.push(&*BLOCK_RANGE_COL);
            }
        }
        Ok(cols)
    }

    /// Create a Diesel select statement that selects the columns in
    /// `columns`. Use to generate a query via
    /// `table.select_cols(columns).filter(...)`. For a full example, see
    /// `Layout::find`
    pub fn select_cols(
        &'a self,
        columns: &[&'a RelColumn],
    ) -> BoxedSelectStatement<'a, Untyped, FromClause<Table<'a>>, Pg> {
        type SelectClause<'b> = DynamicSelectClause<'b, Pg, Table<'b>>;

        fn add_field<'b, ST: SingleValue + Send>(
            select: &mut SelectClause<'b>,
            table: &'b Table<'b>,
            column: &'b RelColumn,
        ) {
            let name = &column.name;

            match (column.is_list(), column.is_nullable()) {
                (true, true) => select.add_field(table.bind::<Nullable<Array<ST>>>(name).unwrap()),
                (true, false) => select.add_field(table.bind::<Array<ST>>(name).unwrap()),
                (false, true) => select.add_field(table.bind::<Nullable<ST>>(name).unwrap()),
                (false, false) => select.add_field(table.bind::<ST>(name).unwrap()),
            }
        }

        fn add_enum_field<'b>(
            select: &mut SelectClause<'b>,
            table: &'b Table<'b>,
            column: &'b RelColumn,
        ) {
            let name = format!("{}.{}::text", table.alias.as_str(), &column.name);

            match (column.is_list(), column.is_nullable()) {
                (true, true) => select.add_field(sql::<Nullable<Array<Text>>>(&name)),
                (true, false) => select.add_field(sql::<Array<Text>>(&name)),
                (false, true) => select.add_field(sql::<Nullable<Text>>(&name)),
                (false, false) => select.add_field(sql::<Text>(&name)),
            }
        }

        let mut selection = DynamicSelectClause::new();
        for column in columns {
            if column.name == TYPENAME_COL.name {
                selection.add_field(sql::<Text>(&format!(
                    "'{}' as __typename",
                    self.meta.object.typename()
                )));
                continue;
            }
            match column.column_type {
                ColumnType::Boolean => add_field::<Bool>(&mut selection, self, column),
                ColumnType::BigDecimal => add_field::<Numeric>(&mut selection, self, column),
                ColumnType::BigInt => add_field::<Numeric>(&mut selection, self, column),
                ColumnType::Bytes => add_field::<Binary>(&mut selection, self, column),
                ColumnType::Int => add_field::<Integer>(&mut selection, self, column),
                ColumnType::Int8 => add_field::<BigInt>(&mut selection, self, column),
                ColumnType::Timestamp => add_field::<Timestamptz>(&mut selection, self, column),
                ColumnType::String => add_field::<Text>(&mut selection, self, column),
                ColumnType::TSVector(_) => {
                    // Skip tsvector columns in SELECT as they are for full-text search only and not
                    // meant to be directly queried or returned
                }
                ColumnType::Enum(_) => add_enum_field(&mut selection, self, column),
            };
        }
        <Self as SelectDsl<SelectClause<'a>>>::select(*self, selection).into_boxed()
    }
}

/// Generate the SQL to use a table in the `from` clause, complete with
/// giving the table an alias
#[derive(Debug, Clone, Copy)]
pub struct FromTable<'a>(Table<'a>);

impl<'a, DB> QueryFragment<DB> for FromTable<'a>
where
    DB: Backend,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();

        out.push_identifier(self.0.meta.nsp.as_str())?;
        out.push_sql(".");
        out.push_identifier(&self.0.meta.name)?;
        out.push_sql(" as ");
        out.push_sql(self.0.alias.as_str());
        Ok(())
    }
}

impl std::fmt::Display for Table<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} as {}", self.meta.name, self.alias.as_str())
    }
}

impl std::fmt::Display for FromTable<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl<'a> QuerySource for Table<'a> {
    type FromClause = FromTable<'a>;
    type DefaultSelection = DummyExpression;

    fn from_clause(&self) -> FromTable<'a> {
        FromTable(*self)
    }

    fn default_selection(&self) -> Self::DefaultSelection {
        DummyExpression::new()
    }
}

impl<'a> AsQuery for Table<'a>
where
    SelectStatement<FromClause<Self>>: Query<SqlType = expression_types::NotSelectable>,
{
    type SqlType = expression_types::NotSelectable;
    type Query = SelectStatement<FromClause<Self>>;

    fn as_query(self) -> Self::Query {
        SelectStatement::simple(self)
    }
}

impl<'a> diesel::Table for Table<'a>
where
    Self: QuerySource + AsQuery,
{
    type PrimaryKey = DummyExpression;
    type AllColumns = DummyExpression;

    fn primary_key(&self) -> Self::PrimaryKey {
        DummyExpression::new()
    }

    fn all_columns() -> Self::AllColumns {
        DummyExpression::new()
    }
}

impl<'a, DB> QueryFragment<DB> for Table<'a>
where
    DB: Backend,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();

        out.push_sql(self.alias.as_str());
        Ok(())
    }
}

impl<'a> QueryId for Table<'a> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

/// Generated by `Table.id_eq`
pub struct IdEq<'a> {
    table: Table<'a>,
    id: &'a Id,
}

impl<'a> IdEq<'a> {
    fn new(table: Table<'a>, id: &'a Id) -> Self {
        IdEq { table, id }
    }
}

impl Expression for IdEq<'_> {
    type SqlType = Bool;
}

impl<'a> QueryFragment<Pg> for IdEq<'a> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();
        self.table.walk_ast(out.reborrow())?;
        out.push_sql(".id = ");
        match self.id {
            Id::String(s) => out.push_bind_param::<Text, _>(s.as_str())?,
            Id::Bytes(b) => out.push_bind_param::<Binary, _>(b)?,
            Id::Int8(i) => out.push_bind_param::<BigInt, _>(i)?,
        }
        Ok(())
    }
}

impl ValidGrouping<()> for IdEq<'_> {
    type IsAggregate = is_aggregate::No;
}

impl<'a> AppearsOnTable<Table<'a>> for IdEq<'a> {}

/// Generated by `Table.block_column`
#[derive(Debug, Clone, Copy)]
pub struct BlockColumn<'a> {
    table: Table<'a>,
}

impl<'a> BlockColumn<'a> {
    fn new(table: Table<'a>) -> Self {
        BlockColumn { table }
    }

    fn immutable(&self) -> bool {
        self.table.meta.immutable
    }

    pub fn name(&self) -> &str {
        if self.immutable() {
            BLOCK_COLUMN
        } else {
            BLOCK_RANGE_COLUMN
        }
    }
}

impl std::fmt::Display for BlockColumn<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}.{}", self.table.alias.as_str(), self.name())
    }
}

impl QueryFragment<Pg> for BlockColumn<'_> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();
        self.table.walk_ast(out.reborrow())?;
        out.push_sql(".");
        out.push_sql(self.name());
        Ok(())
    }
}

/// Generated by `Table.at_block`
#[derive(Debug, Clone, Copy)]
pub struct AtBlock<'a> {
    column: BlockColumn<'a>,
    block: BlockNumber,
    filters_by_id: bool,
}

impl<'a> AtBlock<'a> {
    fn new(table: Table<'a>, block: BlockNumber) -> Self {
        let column = BlockColumn::new(table);
        AtBlock {
            column,
            block,
            filters_by_id: false,
        }
    }

    pub fn filters_by_id(mut self, by_id: bool) -> Self {
        self.filters_by_id = by_id;
        self
    }
}

impl Expression for AtBlock<'_> {
    type SqlType = Bool;
}

impl<'a> QueryFragment<Pg> for AtBlock<'a> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();

        if self.column.immutable() {
            if self.block == BLOCK_NUMBER_MAX {
                // `self.block <= BLOCK_NUMBER_MAX` is always true
                out.push_sql("true");
            } else {
                self.column.walk_ast(out.reborrow())?;
                out.push_sql(" <= ");
                out.push_bind_param::<Integer, _>(&self.block)?;
            }
        } else {
            // Table is mutable and has a block_range column
            self.column.walk_ast(out.reborrow())?;
            out.push_sql(" @> ");
            out.push_bind_param::<Integer, _>(&self.block)?;

            let should_use_brin =
                !self.filters_by_id || ENV_VARS.store.use_brin_for_all_query_types;
            if self.column.table.meta.is_account_like
                && self.block < BLOCK_NUMBER_MAX
                && should_use_brin
            {
                // When block is BLOCK_NUMBER_MAX, these checks would be wrong; we
                // don't worry about adding the equivalent in that case since
                // we generally only see BLOCK_NUMBER_MAX here for metadata
                // queries where block ranges don't matter anyway.
                //
                // We also don't need to add these if the query already filters by ID,
                // because the ideal index is the GiST index on id and block_range.
                out.push_sql(" and coalesce(upper(");
                self.column.walk_ast(out.reborrow())?;
                out.push_sql("), 2147483647) > ");
                out.push_bind_param::<Integer, _>(&self.block)?;
                out.push_sql(" and lower(");
                self.column.walk_ast(out.reborrow())?;
                out.push_sql(") <= ");
                out.push_bind_param::<Integer, _>(&self.block)?;
            }
        }

        Ok(())
    }
}

impl ValidGrouping<()> for AtBlock<'_> {
    type IsAggregate = is_aggregate::No;
}

impl<'a> AppearsOnTable<Table<'a>> for AtBlock<'a> {}

/// Generated by `Table.changed_since`
#[derive(Debug)]
pub struct ChangedSince<'a> {
    column: BlockColumn<'a>,
    block: BlockNumber,
}

impl std::fmt::Display for ChangedSince<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} >= {}", self.column, self.block)
    }
}

impl Expression for ChangedSince<'_> {
    type SqlType = Bool;
}

impl QueryFragment<Pg> for ChangedSince<'_> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        if self.column.table.meta.immutable {
            self.column.walk_ast(out.reborrow())?;
            out.push_sql(" >= ");
            out.push_bind_param::<Integer, _>(&self.block)
        } else {
            out.push_sql("lower(");
            self.column.walk_ast(out.reborrow())?;
            out.push_sql(") >= ");
            out.push_bind_param::<Integer, _>(&self.block)
        }
    }
}

/// Generated by `Table.belongs_to_causality_region`
pub struct BelongsToCausalityRegion<'a> {
    table: Table<'a>,
    cr: CausalityRegion,
}

impl<'a> BelongsToCausalityRegion<'a> {
    fn new(table: Table<'a>, cr: CausalityRegion) -> Self {
        BelongsToCausalityRegion { table, cr }
    }
}

impl Expression for BelongsToCausalityRegion<'_> {
    type SqlType = Bool;
}

impl<'a> QueryFragment<Pg> for BelongsToCausalityRegion<'a> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();

        if self.table.meta.has_causality_region {
            self.table.walk_ast(out.reborrow())?;
            out.push_sql(".causality_region");
            out.push_sql(" = ");
            out.push_bind_param::<Integer, _>(&self.cr)?;
        } else {
            out.push_sql("true");
        }
        Ok(())
    }
}

impl ValidGrouping<()> for BelongsToCausalityRegion<'_> {
    type IsAggregate = is_aggregate::No;
}

impl<'a> AppearsOnTable<Table<'a>> for BelongsToCausalityRegion<'a> {}

/// A specific column in a specific table
#[derive(Debug, Clone, Copy)]
pub struct Column<'a> {
    table: Table<'a>,
    column: &'a super::Column,
}

impl<'a> Column<'a> {
    fn new(table: Table<'a>, column: &'a super::Column) -> Self {
        Column { table, column }
    }

    /// Bind this column to a specific SQL type for use in contexts where
    /// Diesel requires that
    pub fn bind<ST>(&self) -> BoundColumn<'a, ST> {
        BoundColumn::new(self.table, self.column)
    }

    pub fn name(&self) -> &'a str {
        &self.column.name
    }

    pub(crate) fn is_list(&self) -> bool {
        self.column.is_list()
    }

    pub(crate) fn is_primary_key(&self) -> bool {
        self.column.is_primary_key()
    }

    pub(crate) fn is_fulltext(&self) -> bool {
        self.column.is_fulltext()
    }

    pub(crate) fn column_type(&self) -> &'a ColumnType {
        &self.column.column_type
    }

    pub(crate) fn use_prefix_comparison(&self) -> bool {
        self.column.use_prefix_comparison
    }
}

impl std::fmt::Display for Column<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}.{}", self.table.alias.as_str(), self.column.name)
    }
}

impl<'a, DB> QueryFragment<DB> for Column<'a>
where
    DB: Backend,
{
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();
        self.table.walk_ast(out.reborrow())?;
        out.push_sql(".");
        out.push_identifier(&self.column.name)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
/// A database table column bound to the SQL type for the column
pub struct BoundColumn<'a, ST> {
    column: Column<'a>,
    _sql_type: PhantomData<ST>,
}

impl<'a, ST> BoundColumn<'a, ST> {
    fn new(table: Table<'a>, column: &'a super::Column) -> Self {
        let column = Column::new(table, column);
        Self {
            column,
            _sql_type: PhantomData,
        }
    }
}

impl<'a, ST> QueryId for BoundColumn<'a, ST> {
    type QueryId = ();
    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<'a, ST, QS> SelectableExpression<QS> for BoundColumn<'a, ST> where Self: Expression {}

impl<'a, ST, QS> AppearsOnTable<QS> for BoundColumn<'a, ST> where Self: Expression {}

impl<'a, ST> Expression for BoundColumn<'a, ST>
where
    ST: TypedExpressionType,
{
    type SqlType = ST;
}

impl<'a, ST> ValidGrouping<()> for BoundColumn<'a, ST> {
    type IsAggregate = is_aggregate::No;
}

impl<'a, ST, DB> QueryFragment<DB> for BoundColumn<'a, ST>
where
    DB: Backend,
{
    fn walk_ast<'b>(&'b self, out: AstPass<'_, 'b, DB>) -> QueryResult<()> {
        self.column.walk_ast(out)
    }
}
