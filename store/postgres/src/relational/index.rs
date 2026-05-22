//! Parse Postgres index definition into a form that is meaningful for us.
use anyhow::{Error, anyhow};
use graph::env::ENV_VARS;
use std::collections::HashMap;
use std::fmt::{Display, Write};

use diesel::sql_query;
use diesel_async::RunQueryDsl;
use graph::components::store::StoreError;
use graph::itertools::Itertools;
use graph::prelude::{
    BlockNumber, lazy_static,
    regex::{Captures, Regex},
};

use crate::block_range::{BLOCK_COLUMN, BLOCK_RANGE_COLUMN};
use crate::relational::{BYTE_ARRAY_PREFIX_SIZE, STRING_PREFIX_SIZE};
use crate::{AsyncPgConnection, catalog};

use super::{Layout, Table, VID_COLUMN};

#[derive(Clone, Debug, PartialEq)]
pub enum Method {
    Brin,
    BTree,
    Gin,
    Gist,
    Unknown(String),
}

impl Display for Method {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Method::*;
        match self {
            Brin => write!(f, "brin")?,
            BTree => write!(f, "btree")?,
            Gin => write!(f, "gin")?,
            Gist => write!(f, "gist")?,
            Unknown(s) => write!(f, "{s}")?,
        }
        Ok(())
    }
}

impl Method {
    fn parse(method: String) -> Self {
        method.parse().unwrap_or(Method::Unknown(method))
    }
}

impl std::str::FromStr for Method {
    type Err = ();

    fn from_str(method: &str) -> Result<Self, Self::Err> {
        use Method::*;

        match method {
            "brin" => Ok(Brin),
            "btree" => Ok(BTree),
            "gin" => Ok(Gin),
            "gist" => Ok(Gist),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum PrefixKind {
    Left,
    Substring,
}

impl PrefixKind {
    fn parse(kind: &str) -> Option<Self> {
        use PrefixKind::*;

        match kind {
            "substring" => Some(Substring),
            "left" => Some(Left),
            _ => None,
        }
    }

    fn to_sql(&self, name: &str) -> String {
        use PrefixKind::*;

        match self {
            Left => format!("left({name}, {})", STRING_PREFIX_SIZE),
            Substring => format!("substring({name}, 1, {})", BYTE_ARRAY_PREFIX_SIZE),
        }
    }
}

/// An index expression, i.e., a 'column' in an index
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    /// A named column; only user-defined columns appear here
    Column(String),
    /// A prefix of a named column, used for indexes on `text` and `bytea`
    Prefix(String, PrefixKind),
    /// The `vid` column
    Vid,
    /// The `block$` column
    Block,
    /// The `block_range` column
    BlockRange,
    /// The expression `lower(block_range)`
    BlockRangeLower,
    /// The expression `coalesce(upper(block_range), 2147483647)`
    BlockRangeUpper,
    /// The literal index expression since none of the previous options
    /// matched
    Unknown(String),
}

impl Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Column(s) => write!(f, "{s}")?,
            Expr::Prefix(s, _) => write!(f, "{s}")?,
            Expr::Vid => write!(f, "vid")?,
            Expr::Block => write!(f, "{BLOCK_COLUMN}")?,
            Expr::BlockRange => write!(f, "block_range")?,
            Expr::BlockRangeLower => write!(f, "lower(block_range)")?,
            Expr::BlockRangeUpper => write!(f, "upper(block_range)")?,
            Expr::Unknown(e) => write!(f, "{e}")?,
        }
        Ok(())
    }
}

impl Expr {
    const LBR: &str = "lower(block_range)";
    const LBR_MULTI: &str = "lower(block_range) int4_minmax_multi_ops";
    const UBR: &str = "coalesce(upper(block_range), 2147483647)";
    const UBR_MULTI: &str = "coalesce(upper(block_range), 2147483647) int4_minmax_multi_ops";
    const VID_MULTI: &str = "vid int8_minmax_multi_ops";
    const BLOCK_MULTI: &str = "block$ int4_minmax_multi_ops";

    fn parse(expr: &str) -> Self {
        use Expr::*;

        let expr = expr.trim().to_string();

        let prefix_rx = Regex::new("^(?P<kind>substring|left)\\((?P<name>[a-z0-9$_]+)").unwrap();

        // We strip out the minmax_multi_ops operator class from the
        // expression for the columns where we usually use them. They will
        // be put back when we create an index, but whether they should be
        // used depends on the database in which the index is created
        if expr == VID_COLUMN || expr == Self::VID_MULTI {
            Vid
        } else if expr == Self::LBR || expr == Self::LBR_MULTI {
            BlockRangeLower
        } else if expr == Self::UBR || expr == Self::UBR_MULTI {
            BlockRangeUpper
        } else if expr == "block_range" {
            BlockRange
        } else if expr == BLOCK_COLUMN || expr == Self::BLOCK_MULTI {
            Block
        } else if expr
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '$' || c == '_')
        {
            Column(expr)
        } else if let Some(caps) = prefix_rx.captures(&expr) {
            if let Some(name) = caps.name("name") {
                let kind = caps
                    .name("kind")
                    .and_then(|op| PrefixKind::parse(op.as_str()));
                match kind {
                    Some(kind) => Prefix(name.as_str().to_string(), kind),
                    None => Unknown(expr),
                }
            } else {
                Unknown(expr)
            }
        } else {
            Unknown(expr)
        }
    }

    fn is_attribute(&self) -> bool {
        use Expr::*;

        match self {
            Column(_) | Prefix(_, _) => true,
            Vid | Block | BlockRange | BlockRangeLower | BlockRangeUpper | Unknown(_) => false,
        }
    }

    fn is_id(&self) -> bool {
        use Expr::*;
        match self {
            Column(s) => s == "id",
            _ => false,
        }
    }

    /// Here we check if all the columns expressions of the two indexes are "kind of same".
    /// We ignore the operator class of the expression by checking if the string of the
    /// original expression is a prexif of the string of the current one.
    fn is_same_kind_columns(current: &[Expr], orig: &[Expr]) -> bool {
        if orig.len() != current.len() {
            return false;
        }
        for i in 0..orig.len() {
            let o = orig[i].to_sql(false);
            let n = current[i].to_sql(false);

            // check that string n starts with o
            if n.len() < o.len() || n[0..o.len()] != o {
                return false;
            }
        }
        true
    }

    /// Generate a SQL expression for this index expression. The `multi_ops`
    /// indicates whether we should also print the `minmax_multi_ops`
    /// operator class used for BRIN indexes. This is needed because it is
    /// not the default operator class, and only supported in Postgres 14+.
    fn to_sql(&self, multi_ops: bool) -> String {
        match (self, multi_ops) {
            (Expr::Column(name), _) => format!("\"{}\"", name),
            (Expr::Prefix(name, kind), _) => kind.to_sql(&format!("\"{}\"", name)),
            (Expr::Vid, true) => Self::VID_MULTI.to_string(),
            (Expr::Vid, false) => VID_COLUMN.to_string(),
            (Expr::Block, false) => BLOCK_COLUMN.to_string(),
            (Expr::Block, true) => Self::BLOCK_MULTI.to_string(),
            (Expr::BlockRange, _) => BLOCK_RANGE_COLUMN.to_string(),
            (Expr::BlockRangeLower, false) => Self::LBR.to_string(),
            (Expr::BlockRangeLower, true) => Self::LBR_MULTI.to_string(),
            (Expr::BlockRangeUpper, false) => Self::UBR.to_string(),
            (Expr::BlockRangeUpper, true) => Self::UBR_MULTI.to_string(),
            (Expr::Unknown(expr), _) => expr.to_string(),
        }
    }
}

/// The condition for a partial index, i.e., the statement after `where ..`
/// in a `create index` statement
#[derive(Clone, Debug, PartialEq)]
pub enum Cond {
    /// The expression `coalesce(upper(block_range), 2147483647) > $number`
    Partial(BlockNumber),
    /// The expression `coalesce(upper(block_range), 2147483647) < 2147483647`
    Closed,
    /// Any other expression
    Unknown(String),
}

impl Display for Cond {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Cond::*;

        match self {
            Partial(number) => write!(f, "upper(block_range) > {number}"),
            Closed => write!(f, "closed(block_range)"),
            Unknown(s) => write!(f, "{s}"),
        }
    }
}

impl Cond {
    fn parse(cond: String) -> Self {
        fn parse_partial(cond: &str) -> Option<Cond> {
            let cond_rx =
                Regex::new("coalesce\\(upper\\(block_range\\), 2147483647\\) > (?P<number>[0-9]+)")
                    .unwrap();

            let caps = cond_rx.captures(cond)?;
            caps.name("number")
                .map(|number| number.as_str())
                .and_then(|number| number.parse::<BlockNumber>().ok())
                .map(Cond::Partial)
        }

        let cond = cond.trim();
        let cond = if cond.starts_with("(") && cond.ends_with(")") {
            &cond[1..cond.len() - 1]
        } else {
            cond
        };
        let cond = cond.trim();
        if cond == "coalesce(upper(block_range), 2147483647) < 2147483647" {
            Cond::Closed
        } else {
            parse_partial(cond).unwrap_or(Cond::Unknown(cond.to_string()))
        }
    }

    fn to_sql(&self) -> String {
        match self {
            Cond::Partial(number) => {
                format!("(coalesce(upper(block_range), 2147483647) > {number})")
            }
            Cond::Closed => "(coalesce(upper(block_range), 2147483647) < 2147483647)".to_string(),
            Cond::Unknown(cond) => cond.to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum CreateIndex {
    /// The literal index definition passed to `parse`. This is used when we
    /// can't parse a `create index` statement, e.g. because it uses
    /// features we don't care about.
    Unknown { defn: String },
    /// Representation of a `create index` statement that we successfully
    /// parsed.
    Parsed {
        /// Is this a `unique` index
        unique: bool,
        /// The name of the index
        name: String,
        /// The namespace of the table to which this index belongs
        nsp: String,
        /// The name of the table to which this index belongs
        table: String,
        /// The index method
        method: Method,
        /// The columns (or more generally expressions) that are indexed
        columns: Vec<Expr>,
        /// The condition for partial indexes
        cond: Option<Cond>,
        /// Storage parameters for the index
        with: Option<String>,
    },
}

impl Display for CreateIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use CreateIndex::*;

        match self {
            Unknown { defn } => {
                writeln!(f, "{defn}")?;
            }
            Parsed {
                unique,
                name,
                nsp: _,
                table: _,
                method,
                columns,
                cond,
                with,
            } => {
                let columns = columns.iter().map(|c| c.to_string()).join(", ");
                let unique = if *unique { "[uq]" } else { "" };
                write!(f, "{name}{unique} {method}({columns})")?;
                if let Some(cond) = cond {
                    write!(f, " where {cond}")?;
                }
                if let Some(with) = with {
                    write!(f, " with {with}")?;
                }
                writeln!(f)?;
            }
        }
        Ok(())
    }
}

impl CreateIndex {
    /// Parse a `create index` statement. We are mostly concerned with
    /// parsing indexes that `graph-node` created. If we can't parse an
    /// index definition, it is returned as `CreateIndex::Unknown`.
    ///
    ///  The `defn` should be formatted the way it is formatted in Postgres'
    /// `pg_indexes.indexdef` system catalog; it's likely that deviating
    /// from that formatting will make the index definition not parse
    /// properly and return a `CreateIndex::Unknown`.
    pub fn parse(mut defn: String) -> Self {
        fn field(cap: &Captures, name: &str) -> Option<String> {
            cap.name(name).map(|mtch| mtch.as_str().to_string())
        }

        fn split_columns(s: &str) -> Vec<Expr> {
            let mut parens = 0;
            let mut column = String::new();
            let mut columns = Vec::new();

            for c in s.chars() {
                match c {
                    '"' => { /* strip double quotes */ }
                    '(' => {
                        parens += 1;
                        column.push(c);
                    }
                    ')' => {
                        parens -= 1;
                        column.push(c);
                    }
                    ',' if parens == 0 => {
                        columns.push(Expr::parse(&column));
                        column = String::new();
                    }
                    _ => column.push(c),
                }
            }
            columns.push(Expr::parse(&column));

            columns
        }

        fn new_parsed(defn: &str) -> Option<CreateIndex> {
            let rx = Regex::new(
                "create (?P<unique>unique )?index (?P<name>\"?[a-z0-9$_]+\"?) \
            on (?P<nsp>sgd[0-9]+)\\.(?P<table>\"?[a-z0-9$_]+\"?) \
            using (?P<method>[a-z]+) \\((?P<columns>.*?)\\)\
            ( with \\((?P<with>.*)\\))?\
            ( where (?P<cond>.*))?$",
            )
            .unwrap();

            let cap = rx.captures(defn)?;
            let unique = cap.name("unique").is_some();
            let name = field(&cap, "name")?;
            let nsp = field(&cap, "nsp")?;
            let table = field(&cap, "table")?;
            let columns = field(&cap, "columns")?;
            let method = Method::parse(field(&cap, "method")?);
            let cond = field(&cap, "cond").map(Cond::parse);
            let with = field(&cap, "with");

            let columns = split_columns(&columns);
            Some(CreateIndex::Parsed {
                unique,
                name,
                nsp,
                table,
                method,
                columns,
                cond,
                with,
            })
        }

        defn.make_ascii_lowercase();
        new_parsed(&defn).unwrap_or(CreateIndex::Unknown { defn })
    }

    pub fn create<C: Into<Vec<Expr>>>(
        name: &str,
        nsp: &str,
        table: &str,
        unique: bool,
        method: Method,
        columns: C,
        cond: Option<Cond>,
        with: Option<String>,
    ) -> Self {
        CreateIndex::Parsed {
            unique,
            name: name.to_string(),
            nsp: nsp.to_string(),
            table: table.to_string(),
            method,
            columns: columns.into(),
            cond,
            with,
        }
    }

    pub(crate) fn with_nsp(&self, nsp2: String) -> Result<Self, Error> {
        let s = self.clone();
        match s {
            CreateIndex::Unknown { defn: _ } => Err(anyhow!("Failed to parse the index")),
            CreateIndex::Parsed {
                unique,
                name,
                nsp: _,
                table,
                method,
                columns,
                cond,
                with,
            } => Ok(CreateIndex::Parsed {
                unique,
                name,
                nsp: nsp2,
                table,
                method,
                columns,
                cond,
                with,
            }),
        }
    }

    pub fn is_attribute_index(&self) -> bool {
        use CreateIndex::*;
        match self {
            Unknown { defn: _ } => false,
            Parsed {
                columns,
                cond,
                with,
                method,
                ..
            } => {
                if cond.is_some() || with.is_some() {
                    return false;
                }
                match method {
                    Method::Gist => {
                        columns.len() == 2
                            && columns[0].is_attribute()
                            && !columns[0].is_id()
                            && columns[1] == Expr::BlockRange
                    }
                    Method::Brin => false,
                    Method::Gin => {
                        // 'using gin(<attr>)'
                        columns.len() == 1
                            && columns[0].is_attribute()
                            && cond.is_none()
                            && with.is_none()
                    }
                    Method::BTree => {
                        match columns.len() {
                            1 => {
                                // 'using btree(<attr>)'
                                columns[0].is_attribute() && cond.is_none() && with.is_none()
                            }
                            2 => {
                                // 'using btree(<attr>, block$)'
                                columns[0].is_attribute()
                                    && columns[1] == Expr::Block
                                    && cond.is_none()
                                    && with.is_none()
                            }
                            _ => false,
                        }
                    }
                    Method::Unknown(_) => false,
                }
            }
        }
    }

    pub fn is_default_non_attr_index(&self) -> bool {
        lazy_static! {
            static ref DEFAULT_INDEXES: Vec<CreateIndex> = {
                fn dummy(
                    unique: bool,
                    method: Method,
                    columns: &[Expr],
                    cond: Option<Cond>,
                ) -> CreateIndex {
                    CreateIndex::create(
                        "dummy_index",
                        "dummy_nsp",
                        "dummy_table",
                        unique,
                        method,
                        columns,
                        cond,
                        None,
                    )
                }
                use Method::*;

                vec![
                    dummy(
                        false,
                        Brin,
                        &[Expr::BlockRangeLower, Expr::BlockRangeUpper, Expr::Vid],
                        None,
                    ),
                    dummy(true, BTree, &[Expr::Vid], None),
                    dummy(
                        false,
                        Gist,
                        &[Expr::Column("id".to_string()), Expr::BlockRange],
                        None,
                    ),
                    dummy(false, BTree, &[Expr::BlockRangeUpper], Some(Cond::Closed)),
                    dummy(false, BTree, &[Expr::Block], None),
                ]
            };
        }

        DEFAULT_INDEXES.iter().any(|idx| self.is_same_index(idx))
    }

    /// Return `true` if `self` is one of the indexes we create by default
    pub fn is_default_index(&self) -> bool {
        self.is_attribute_index() || self.is_default_non_attr_index()
    }

    fn is_same_index(&self, other: &CreateIndex) -> bool {
        match (self, other) {
            (CreateIndex::Unknown { .. }, _) | (_, CreateIndex::Unknown { .. }) => false,
            (
                CreateIndex::Parsed {
                    unique,
                    name: _,
                    nsp: _,
                    table: _,
                    method,
                    columns,
                    cond,
                    with,
                },
                CreateIndex::Parsed {
                    unique: o_unique,
                    name: _,
                    nsp: _,
                    table: _,
                    method: o_method,
                    columns: o_columns,
                    cond: o_cond,
                    with: o_with,
                },
            ) => {
                unique == o_unique
                    && method == o_method
                    && Expr::is_same_kind_columns(columns, o_columns)
                    && cond == o_cond
                    && with == o_with
            }
        }
    }

    pub fn is_id(&self) -> bool {
        // on imutable tables the id constraint is specified at table creation
        match self {
            CreateIndex::Unknown { .. } => (),
            CreateIndex::Parsed { columns, .. } => {
                if columns.len() == 1 && columns[0].is_id() {
                    return true;
                }
            }
        }
        false
    }

    pub fn to_postpone(&self) -> bool {
        if !ENV_VARS.postpone_attribute_index_creation {
            return false;
        }

        fn has_prefix(s: &str, prefix: &str) -> bool {
            s.starts_with(prefix)
                || s.ends_with("\"") && s.starts_with(format!("\"{}", prefix).as_str())
        }
        match self {
            CreateIndex::Unknown { .. } => false,
            CreateIndex::Parsed {
                name,
                columns,
                method,
                ..
            } => {
                if *method != Method::BTree {
                    return false;
                }
                if columns.len() == 1 && columns[0].is_id() {
                    return false;
                }
                has_prefix(name, "attr_") && self.is_attribute_index()
            }
        }
    }

    pub fn name(&self) -> Option<String> {
        match self {
            CreateIndex::Unknown { .. } => None,
            CreateIndex::Parsed { name, .. } => Some(name.clone()),
        }
    }

    /// Return `true` if any of the attribute columns (user-defined columns,
    /// not infrastructure columns like `vid`, `block$`, etc.) referenced by
    /// this index are NOT in `columns`.
    pub fn references_column_not_in(&self, columns: &std::collections::HashSet<&str>) -> bool {
        match self {
            CreateIndex::Unknown { .. } => false,
            CreateIndex::Parsed { columns: exprs, .. } => exprs.iter().any(|expr| match expr {
                Expr::Column(name) | Expr::Prefix(name, _) => !columns.contains(name.as_str()),
                _ => false,
            }),
        }
    }

    pub fn fields_exist_in_dest(&self, dest_table: &Table) -> bool {
        fn column_exists(dest_table: &Table, column_name: &str) -> bool {
            dest_table
                .columns
                .iter()
                .any(|c| c.name.as_str() == column_name)
        }

        match self {
            CreateIndex::Unknown { defn: _ } => return true,
            CreateIndex::Parsed {
                columns: parsed_cols,
                ..
            } => {
                for c in parsed_cols {
                    match c {
                        Expr::Column(column_name) => {
                            if !column_exists(dest_table, column_name) {
                                return false;
                            }
                        }
                        Expr::Prefix(column_name, _) => {
                            if !column_exists(dest_table, column_name) {
                                return false;
                            }
                        }
                        Expr::BlockRange | Expr::BlockRangeLower | Expr::BlockRangeUpper => {
                            if dest_table.immutable {
                                return false;
                            }
                        }
                        Expr::Vid => (),
                        Expr::Block => {
                            if !dest_table.immutable {
                                return false;
                            }
                        }
                        Expr::Unknown(expression) => {
                            if expression.contains("block_range") && dest_table.immutable {
                                return false;
                            }
                            if !dest_table
                                .columns
                                .iter()
                                .any(|c| expression.contains(c.name.as_str()))
                                && !expression.contains("block_range")
                                && !expression.contains("vid")
                            {
                                return false;
                            }
                        }
                    }
                }
            }
        }
        true
    }

    /// Generate a SQL statement that creates this index. If `concurrent` is
    /// `true`, make it a concurrent index creation. If `if_not_exists` is
    /// `true` add a `if not exists` clause to the index creation.
    fn to_sql(&self, creat: &IndexCreator) -> Result<String, std::fmt::Error> {
        match self {
            CreateIndex::Unknown { defn } => Ok(defn.to_string()),
            CreateIndex::Parsed {
                unique,
                name,
                nsp,
                table,
                method,
                columns,
                cond,
                with,
            } => {
                let IndexCreator {
                    concurrently,
                    if_not_exists,
                    multi_ops,
                } = creat;
                // Explicit operator classes are only needed for BRIN indexes
                let multi_ops = *multi_ops && method == &Method::Brin;
                let unique = if *unique { "unique " } else { "" };
                let concurrent = if *concurrently { "concurrently " } else { "" };
                let if_not_exists = if *if_not_exists { "if not exists " } else { "" };
                let columns = columns.iter().map(|c| c.to_sql(multi_ops)).join(", ");

                let mut sql = format!(
                    "create {unique}index {concurrent}{if_not_exists}{name} on {nsp}.{table} using {method} ({columns})"
                );
                if let Some(with) = with {
                    write!(sql, " with ({with})")?;
                }
                if let Some(cond) = cond {
                    write!(sql, " where {}", cond.to_sql())?;
                }
                Ok(sql)
            }
        }
    }

    pub(crate) fn method(self, method: Method) -> CreateIndex {
        match self {
            CreateIndex::Unknown { defn } => CreateIndex::Unknown { defn },
            CreateIndex::Parsed {
                unique,
                name,
                nsp,
                table,
                method: _,
                columns,
                cond,
                with,
            } => CreateIndex::Parsed {
                unique,
                name,
                nsp,
                table,
                method,
                columns,
                cond,
                with,
            },
        }
    }

    pub(crate) fn cond(self, cond: Cond) -> CreateIndex {
        match self {
            CreateIndex::Unknown { defn } => CreateIndex::Unknown { defn },
            CreateIndex::Parsed {
                unique,
                name,
                nsp,
                table,
                method,
                columns,
                cond: _,
                with,
            } => CreateIndex::Parsed {
                unique,
                name,
                nsp,
                table,
                method,
                columns,
                cond: Some(cond),
                with,
            },
        }
    }
}

/// A helper to run or write index creation statements with options as to
/// whether to create them concurrently or only of they do not exist
pub struct IndexCreator {
    concurrently: bool,
    if_not_exists: bool,
    /// Whether the shard supports the multi_ops operator classes
    multi_ops: bool,
}

impl IndexCreator {
    /// Create an index creator with the given options. The `multi_ops` flag
    /// indicates whether the database in which we will create indexes
    /// supports the `minmax_multi_ops` operator classes
    pub fn new(concurrently: bool, if_not_exists: bool, multi_ops: bool) -> Self {
        IndexCreator {
            concurrently,
            if_not_exists,
            multi_ops,
        }
    }

    /// Create the index `idx` in the transaction that is currently active
    /// on `conn`, i.e., do not start a new transaction
    pub async fn execute(
        &self,
        conn: &mut AsyncPgConnection,
        idx: &CreateIndex,
    ) -> Result<(), StoreError> {
        let sql = idx.to_sql(self)?;
        sql_query(sql).execute(conn).await?;
        Ok(())
    }

    /// Create all indexes in `idxs`. Each index creation happens in its own
    /// transaction, and `conn` should therefore not be in a transaction
    /// when this method is called.
    pub async fn execute_many(
        &self,
        conn: &mut AsyncPgConnection,
        idxs: &[CreateIndex],
    ) -> Result<(), StoreError> {
        for idx in idxs {
            self.execute(conn, idx).await?;
        }
        Ok(())
    }

    pub fn to_sql(&self, index: &CreateIndex) -> Result<String, std::fmt::Error> {
        index.to_sql(self)
    }
}

impl Layout {
    /// Create an index creator with the given options for creating indexes
    /// in this layout
    pub fn index_creator(&self, concurrently: bool, if_not_exists: bool) -> IndexCreator {
        let multi_ops = self.catalog.has_minmax_multi_ops;
        IndexCreator::new(concurrently, if_not_exists, multi_ops)
    }
}

#[derive(Debug)]
pub struct IndexList {
    pub(crate) indexes: HashMap<String, Vec<CreateIndex>>,
}

impl IndexList {
    pub fn new(indexes: HashMap<String, Vec<CreateIndex>>) -> Self {
        IndexList { indexes }
    }

    pub async fn load(conn: &mut AsyncPgConnection, layout: &Layout) -> Result<Self, StoreError> {
        let mut indexes = HashMap::new();
        let schema_name = layout.site.namespace.clone();
        for table in layout.tables.values() {
            let indexes_from_table =
                catalog::indexes_for_table(conn, schema_name.as_str(), table.name.as_str())
                    .await?
                    .into_iter()
                    .map(CreateIndex::parse)
                    .collect();
            indexes.insert(table.name.to_string(), indexes_from_table);
        }
        Ok(Self::new(indexes))
    }

    /// Return all indexes for `table` from this list. Only indexes that are
    /// for columns that actually exist on `table` are returned. In
    /// addition, indexes that are always created when a deployment is
    /// created independent of the configuration are also not returned.
    pub fn indexes_for_table<'a>(
        &'a self,
        table: &'a Table,
    ) -> impl Iterator<Item = &'a CreateIndex> {
        static EMPTY: Vec<CreateIndex> = vec![];
        let indexes = self.indexes.get(table.name.as_str()).unwrap_or(&EMPTY);

        indexes.iter().filter(move |ci| {
            // First we check if the fields do exist in the destination subgraph.
            // In case of grafting that is not given.
            ci.fields_exist_in_dest(table)
                // Then we check if the index is one of the default indexes not based on
                // the attributes. Those will be created anyway and we should skip them.
                && !ci.is_default_non_attr_index()
                // Then ID based indexes in the immutable tables are also created initially
                // and should be skipped.
                && !(ci.is_id() && table.immutable)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use graph::prelude::{BlockNumber, DeploymentHash};
    use graph::schema::InputSchema;

    use crate::layout_for_tests::{Namespace, make_dummy_site};
    use crate::relational::Catalog;

    use super::*;

    fn test_layout(gql: &str) -> Layout {
        let subgraph = DeploymentHash::new("subgraph").unwrap();
        let schema = InputSchema::parse_latest(gql, subgraph.clone()).expect("Test schema invalid");
        let namespace = Namespace::new("sgd0815".to_owned()).unwrap();
        let site = Arc::new(make_dummy_site(subgraph, namespace, "anet".to_string()));
        let catalog =
            Catalog::for_tests(site.clone(), BTreeSet::new()).expect("Can not create catalog");
        Layout::new(site, &schema, catalog).expect("Failed to construct Layout")
    }

    #[test]
    fn parse() {
        use Method::*;

        #[derive(Debug)]
        enum TestExpr {
            Name(&'static str),
            Prefix(&'static str, &'static str),
            Vid,
            Block,
            BlockRange,
            BlockRangeLower,
            BlockRangeUpper,
            #[allow(dead_code)]
            Unknown(&'static str),
        }

        impl<'a> From<&'a TestExpr> for Expr {
            fn from(expr: &'a TestExpr) -> Self {
                match expr {
                    TestExpr::Name(name) => Expr::Column(name.to_string()),
                    TestExpr::Prefix(name, kind) => {
                        Expr::Prefix(name.to_string(), PrefixKind::parse(kind).unwrap())
                    }
                    TestExpr::Vid => Expr::Vid,
                    TestExpr::Block => Expr::Block,
                    TestExpr::BlockRange => Expr::BlockRange,
                    TestExpr::BlockRangeLower => Expr::BlockRangeLower,
                    TestExpr::BlockRangeUpper => Expr::BlockRangeUpper,
                    TestExpr::Unknown(s) => Expr::Unknown(s.to_string()),
                }
            }
        }

        #[derive(Debug, Clone)]
        enum TestCond {
            Partial(BlockNumber),
            Closed,
            Unknown(&'static str),
        }

        impl From<TestCond> for Cond {
            fn from(expr: TestCond) -> Self {
                match expr {
                    TestCond::Partial(number) => Cond::Partial(number),
                    TestCond::Unknown(s) => Cond::Unknown(s.to_string()),
                    TestCond::Closed => Cond::Closed,
                }
            }
        }

        #[derive(Debug, Clone)]
        struct Parsed {
            unique: bool,
            name: &'static str,
            nsp: &'static str,
            table: &'static str,
            method: Method,
            columns: &'static [TestExpr],
            cond: Option<TestCond>,
        }

        impl From<Parsed> for CreateIndex {
            fn from(p: Parsed) -> Self {
                let Parsed {
                    unique,
                    name,
                    nsp,
                    table,
                    method,
                    columns,
                    cond,
                } = p;
                let columns: Vec<_> = columns.iter().map(Expr::from).collect();
                let cond = cond.map(Cond::from);
                CreateIndex::Parsed {
                    unique,
                    name: name.to_string(),
                    nsp: nsp.to_string(),
                    table: table.to_string(),
                    method,
                    columns,
                    cond,
                    with: None,
                }
            }
        }

        #[track_caller]
        fn parses_to(defn: &str, exp: &Parsed) -> CreateIndex {
            let act = CreateIndex::parse(defn.to_string());
            let exp = CreateIndex::from(exp.clone());
            assert_eq!(exp, act);
            act
        }

        #[track_caller]
        fn parse_one(defn: &str, exp: Parsed) {
            let act = parses_to(defn, &exp);

            let defn = defn.to_ascii_lowercase();
            let creat = IndexCreator::new(false, false, false);
            assert_eq!(defn, creat.to_sql(&act).unwrap());
        }

        // Test that the equivalent index definitions in `defns` are parsed to
        // the same `CreateIndex` and that turning those index definitions into
        // a SQL string produces `defns[0]`
        #[track_caller]
        fn parse_many(defns: &[&str], exp: Parsed) {
            let act = parses_to(defns[0], &exp);
            for defn in &defns[1..] {
                parses_to(defn, &exp);
            }

            let defn = defns[0].to_ascii_lowercase();
            let creat = IndexCreator::new(false, false, false);
            assert_eq!(defn, creat.to_sql(&act).unwrap());
        }

        use TestCond::*;
        use TestExpr::*;

        let sql = "create index attr_1_0_token_id on sgd44.token using btree (\"id\")";
        let exp = Parsed {
            unique: false,
            name: "attr_1_0_token_id",
            nsp: "sgd44",
            table: "token",
            method: BTree,
            columns: &[Name("id")],
            cond: None,
        };
        parse_one(sql, exp);

        let sql =
            "create index attr_1_1_token_symbol on sgd44.token using btree (left(\"symbol\", 256))";
        let exp = Parsed {
            unique: false,
            name: "attr_1_1_token_symbol",
            nsp: "sgd44",
            table: "token",
            method: BTree,
            columns: &[Prefix("symbol", "left")],
            cond: None,
        };
        parse_one(sql, exp);

        let sql = "create index attr_1_5_token_trade_volume on sgd44.token using btree (\"trade_volume\")";
        let exp = Parsed {
            unique: false,
            name: "attr_1_5_token_trade_volume",
            nsp: "sgd44",
            table: "token",
            method: BTree,
            columns: &[Name("trade_volume")],
            cond: None,
        };
        parse_one(sql, exp);

        let sql = "create unique index token_pkey on sgd44.token using btree (vid)";
        let exp = Parsed {
            unique: true,
            name: "token_pkey",
            nsp: "sgd44",
            table: "token",
            method: BTree,
            columns: &[Vid],
            cond: None,
        };
        parse_one(sql, exp);

        let sql = "create index brin_token on sgd44.token using brin (lower(block_range), coalesce(upper(block_range), 2147483647), vid)";
        let exp = Parsed {
            unique: false,
            name: "brin_token",
            nsp: "sgd44",
            table: "token",
            method: Brin,
            columns: &[BlockRangeLower, BlockRangeUpper, Vid],
            cond: None,
        };
        parse_one(sql, exp);

        let sql = "CREATE INDEX brin_nft_transfer ON sgd4.nft_transfer USING brin (lower(block_range) int4_minmax_multi_ops, COALESCE(upper(block_range), 2147483647) int4_minmax_multi_ops, vid int8_minmax_multi_ops)";
        let act = CreateIndex::parse(sql.to_string());
        let CreateIndex::Parsed { columns, .. } = act else {
            panic!("Failed to parse index");
        };
        assert_eq!(
            vec![Expr::BlockRangeLower, Expr::BlockRangeUpper, Expr::Vid],
            columns
        );

        let sql = "create index token_block_range_closed on sgd44.token using btree (coalesce(upper(block_range), 2147483647)) where (coalesce(upper(block_range), 2147483647) < 2147483647)";
        let exp = Parsed {
            unique: false,
            name: "token_block_range_closed",
            nsp: "sgd44",
            table: "token",
            method: BTree,
            columns: &[BlockRangeUpper],
            cond: Some(Closed),
        };
        parse_one(sql, exp);

        let sql = "create index token_id_block_range_excl on sgd44.token using gist (\"id\", block_range)";
        let exp = Parsed {
            unique: false,
            name: "token_id_block_range_excl",
            nsp: "sgd44",
            table: "token",
            method: Gist,
            columns: &[Name("id"), BlockRange],
            cond: None,
        };
        parse_one(sql, exp);

        let sql = "create index attr_1_11_pool_owner on sgd411585.pool using btree (substring(\"owner\", 1, 64))";
        let exp = Parsed {
            unique: false,
            name: "attr_1_11_pool_owner",
            nsp: "sgd411585",
            table: "pool",
            method: BTree,
            columns: &[Prefix("owner", "substring")],
            cond: None,
        };
        parse_one(sql, exp);

        let sql = "create index attr_1_20_pool_vault_id on sgd411585.pool using gist (\"vault_id\", block_range)";
        let exp = Parsed {
            unique: false,
            name: "attr_1_20_pool_vault_id",
            nsp: "sgd411585",
            table: "pool",
            method: Gist,
            columns: &[Name("vault_id"), BlockRange],
            cond: None,
        };
        parse_one(sql, exp);

        let sql =
            "create index attr_1_22_pool_tokens_list on sgd411585.pool using gin (\"tokens_list\")";
        let exp = Parsed {
            unique: false,
            name: "attr_1_22_pool_tokens_list",
            nsp: "sgd411585",
            table: "pool",
            method: Gin,
            columns: &[Name("tokens_list")],
            cond: None,
        };
        parse_one(sql, exp);

        let sql = "create index manual_partial_pool_total_liquidity on sgd411585.pool using btree (\"total_liquidity\") where (coalesce(upper(block_range), 2147483647) > 15635000)";
        let exp = Parsed {
            unique: false,
            name: "manual_partial_pool_total_liquidity",
            nsp: "sgd411585",
            table: "pool",
            method: BTree,
            columns: &[Name("total_liquidity")],
            cond: Some(Partial(15635000)),
        };
        parse_one(sql, exp);

        let sql = "create index manual_swap_pool_timestamp_id on sgd217942.swap using btree (\"pool\", \"timestamp\", \"id\")";
        let exp = Parsed {
            unique: false,
            name: "manual_swap_pool_timestamp_id",
            nsp: "sgd217942",
            table: "swap",
            method: BTree,
            columns: &[Name("pool"), Name("timestamp"), Name("id")],
            cond: None,
        };
        parse_one(sql, exp);

        let sql = "CREATE INDEX brin_scy ON sgd314614.scy USING brin (block$, vid)";
        let exp = Parsed {
            unique: false,
            name: "brin_scy",
            nsp: "sgd314614",
            table: "scy",
            method: Brin,
            columns: &[Block, Vid],
            cond: None,
        };
        parse_one(sql, exp);

        let sqls = &[
            "CREATE INDEX brin_scy ON sgd314614.scy USING brin (block$, vid) where amount > 0",
            "CREATE INDEX brin_scy ON sgd314614.scy USING brin (block$, vid) where (amount > 0)",
        ];
        let exp = Parsed {
            unique: false,
            name: "brin_scy",
            nsp: "sgd314614",
            table: "scy",
            method: Brin,
            columns: &[Block, Vid],
            cond: Some(TestCond::Unknown("amount > 0")),
        };
        parse_many(sqls, exp);

        let sqls = &[
            "CREATE INDEX manual_token_random_cond ON sgd44.token USING btree (\"decimals\") WHERE decimals > (5)::numeric",
            "CREATE INDEX manual_token_random_cond ON sgd44.token USING btree (decimals) WHERE decimals > (5)::numeric",
            "CREATE INDEX manual_token_random_cond ON sgd44.token USING btree (decimals) WHERE ( decimals > (5)::numeric )",
            "CREATE INDEX manual_token_random_cond ON sgd44.token USING btree (\"decimals\") WHERE ( decimals > (5)::numeric )",
        ];
        let exp = Parsed {
            unique: false,
            name: "manual_token_random_cond",
            nsp: "sgd44",
            table: "token",
            method: BTree,
            columns: &[Name("decimals")],
            cond: Some(TestCond::Unknown("decimals > (5)::numeric")),
        };
        parse_many(sqls, exp);

        let sqls = &[
            "CREATE INDEX manual_pool_swap_enabled_total_liquidity ON sgd12.pool USING btree (\"total_liquidity\") WHERE swap_enabled",
            "CREATE INDEX manual_pool_swap_enabled_total_liquidity ON sgd12.pool USING btree (total_liquidity) WHERE swap_enabled",
        ];
        let exp = Parsed {
            unique: false,
            name: "manual_pool_swap_enabled_total_liquidity",
            nsp: "sgd12",
            table: "pool",
            method: BTree,
            columns: &[Name("total_liquidity")],
            cond: Some(TestCond::Unknown("swap_enabled")),
        };
        parse_many(sqls, exp);
    }

    #[test]
    fn fields_exist_in_dest_out_of_order() {
        let gql = "type Thing @entity {
            id: Bytes!
            early: String!
            mid: String!
            late: String!
        }";
        let layout = test_layout(gql);
        let table = layout
            .table_for_entity(&layout.input_schema.entity_type("Thing").unwrap())
            .unwrap();

        // Index references columns in reverse order vs the table definition.
        // Table columns: id, early, mid, late
        // Index columns: late, mid, early
        // The consuming iterator finds 'late', advances past 'mid' and 'early',
        // then can't find 'mid' because the iterator is already past it.
        let index = CreateIndex::Parsed {
            unique: false,
            name: "test_reverse".to_string(),
            nsp: "sgd0815".to_string(),
            table: "thing".to_string(),
            method: Method::BTree,
            columns: vec![
                Expr::Column("late".to_string()),
                Expr::Column("mid".to_string()),
                Expr::Column("early".to_string()),
            ],
            cond: None,
            with: None,
        };

        assert!(
            index.fields_exist_in_dest(table),
            "index columns exist in table regardless of order"
        );
    }
}
