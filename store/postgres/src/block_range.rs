use diesel::pg::Pg;
use diesel::query_builder::{AstPass, QueryFragment};
use diesel::result::QueryResult;
///! Utilities to deal with block numbers and block ranges
use diesel::sql_types::{Integer, Range};
use std::ops::{Bound, RangeBounds, RangeFrom};

use graph::prelude::{BlockNumber, BlockPtr, BLOCK_NUMBER_MAX};

use crate::relational::Table;

/// The name of the column in which we store the block range for mutable
/// entities
pub(crate) const BLOCK_RANGE_COLUMN: &str = "block_range";

/// The SQL clause we use to check that an entity version is current;
/// that version has an unbounded block range, but checking for
/// `upper_inf(block_range)` is slow and can't use the exclusion
/// index we have on entity tables; we therefore check if i32::MAX is
/// in the range
pub(crate) const BLOCK_RANGE_CURRENT: &str = "block_range @> 2147483647";

/// Most subgraph metadata entities are not versioned. For such entities, we
/// want two things:
///   - any CRUD operation modifies such an entity in place
///   - queries by a block number consider such an entity as present for
///     any block number
/// We therefore mark such entities with a block range `[-1,\infinity)`; we
/// use `-1` as the lower bound to make it easier to identify such entities
/// for troubleshooting/debugging
pub(crate) const BLOCK_UNVERSIONED: i32 = -1;

pub(crate) const UNVERSIONED_RANGE: (Bound<i32>, Bound<i32>) =
    (Bound::Included(BLOCK_UNVERSIONED), Bound::Unbounded);

/// The name of the column in which we store the block from which an
/// immutable entity is visible
pub(crate) const BLOCK_COLUMN: &str = "block$";

/// The range of blocks for which an entity is valid. We need this struct
/// to bind ranges into Diesel queries.
#[derive(Clone, Debug, Copy)]
pub struct BlockRange(Bound<BlockNumber>, Bound<BlockNumber>);

impl BlockRange {
    pub fn first_block(&self) -> Option<BlockNumber> {
        if (self.0, self.1) == UNVERSIONED_RANGE {
            return None;
        }

        match self.0 {
            Bound::Included(nr) => Some(nr),
            Bound::Excluded(nr) => Some(nr + 1),
            Bound::Unbounded => None,
        }
    }
}

impl From<(Bound<BlockNumber>, Bound<BlockNumber>)> for BlockRange {
    fn from(bounds: (Bound<BlockNumber>, Bound<BlockNumber>)) -> Self {
        BlockRange(bounds.0, bounds.1)
    }
}

impl From<RangeFrom<BlockNumber>> for BlockRange {
    fn from(range: RangeFrom<BlockNumber>) -> BlockRange {
        BlockRange(range.start_bound().cloned(), range.end_bound().cloned())
    }
}

/// Return the block number contained in the history event. If it is
/// `None` panic because that indicates that we want to perform an
/// operation that does not record history, which should not happen
/// with how we currently use relational schemas
pub(crate) fn block_number(block_ptr: &BlockPtr) -> BlockNumber {
    block_ptr.number
}

#[derive(Constructor)]
pub struct BlockRangeLowerBoundClause<'a> {
    _table_prefix: &'a str,
    block: BlockNumber,
}

impl<'a> QueryFragment<Pg> for BlockRangeLowerBoundClause<'a> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();

        out.push_sql("lower(");
        out.push_identifier(BLOCK_RANGE_COLUMN)?;
        out.push_sql(") = ");
        out.push_bind_param::<Integer, _>(&self.block)?;

        Ok(())
    }
}

#[derive(Constructor)]
pub struct BlockRangeUpperBoundClause<'a> {
    _table_prefix: &'a str,
    block: BlockNumber,
}

impl<'a> QueryFragment<Pg> for BlockRangeUpperBoundClause<'a> {
    fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();

        out.push_sql("coalesce(upper(");
        out.push_identifier(BLOCK_RANGE_COLUMN)?;
        out.push_sql("), 2147483647) = ");
        out.push_bind_param::<Integer, _>(&self.block)?;

        Ok(())
    }
}

/// Helper for generating various SQL fragments for handling the block range
/// of entity versions
#[derive(Debug, Clone, Copy)]
pub struct BlockRangeColumn<'a> {
    table: &'a Table,
    table_prefix: &'a str,
    block: BlockNumber,
    // The range (block, ..) in a form that we can serialize with Diesel
    block_range: (Bound<BlockNumber>, Bound<BlockNumber>),
}

impl<'a> BlockRangeColumn<'a> {
    pub fn new(table: &'a Table, table_prefix: &'a str, block: BlockNumber) -> Self {
        Self {
            table,
            table_prefix,
            block,
            block_range: (Bound::Included(block), Bound::Unbounded),
        }
    }
}

impl<'a> BlockRangeColumn<'a> {
    /// Output SQL that matches only rows whose block range contains `block`
    pub fn contains<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();

        let Self {
            table,
            table_prefix: _,
            block,
            block_range: _,
        } = self;

        if self.is_immutable() {
            if *block == BLOCK_NUMBER_MAX {
                // `self.block <= BLOCK_NUMBER_MAX` is always true
                out.push_sql("true");
                Ok(())
            } else {
                self.name(out);
                out.push_sql(" <= ");
                out.push_bind_param::<Integer, _>(block)
            }
        } else {
            self.name(out);
            out.push_sql(" @> ");
            out.push_bind_param::<Integer, _>(block)?;
            if table.is_account_like && *block < BLOCK_NUMBER_MAX {
                // When block is BLOCK_NUMBER_MAX, these checks would be wrong; we
                // don't worry about adding the equivalent in that case since
                // we generally only see BLOCK_NUMBER_MAX here for metadata
                // queries where block ranges don't matter anyway
                out.push_sql(" and coalesce(upper(");
                out.push_identifier(BLOCK_RANGE_COLUMN)?;
                out.push_sql("), 2147483647) > ");
                out.push_bind_param::<Integer, _>(block)?;
                out.push_sql(" and lower(");
                out.push_identifier(BLOCK_RANGE_COLUMN)?;
                out.push_sql(") <= ");
                out.push_bind_param::<Integer, _>(block)
            } else {
                Ok(())
            }
        }
    }

    pub fn is_immutable(&self) -> bool {
        self.table.immutable
    }

    pub fn column_name(&self) -> &str {
        if self.is_immutable() {
            BLOCK_COLUMN
        } else {
            BLOCK_RANGE_COLUMN
        }
    }

    /// Output the qualified name of the block range column
    pub fn name(&self, out: &mut AstPass<Pg>) {
        out.push_sql(self.table_prefix);
        out.push_sql(self.column_name());
    }

    /// Output the name of the block range column without the table prefix
    pub(crate) fn bare_name(&self, out: &mut AstPass<Pg>) {
        out.push_sql(self.column_name());
    }

    /// Output the literal value of the block range `[block,..)`, mostly for
    /// generating an insert statement containing the block range column
    pub fn literal_range_current<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        if self.is_immutable() {
            out.push_bind_param::<Integer, _>(&self.block)
        } else {
            out.push_bind_param::<Range<Integer>, _>(&self.block_range)
        }
    }

    /// Output an expression that matches rows that are the latest version
    /// of their entity
    pub fn latest(&self, out: &mut AstPass<Pg>) {
        if self.is_immutable() {
            out.push_sql("true");
        } else {
            out.push_sql(BLOCK_RANGE_CURRENT);
        }
    }

    /// Output SQL that updates the block range column so that the row is
    /// only valid up to `block` (exclusive)
    ///
    /// # Panics
    ///
    /// If the underlying table is immutable, this method will panic
    pub fn clamp<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        assert!(
            !self.is_immutable(),
            "immutable entities can not be updated or deleted"
        );

        self.name(out);
        out.push_sql(" = int4range(lower(");
        out.push_identifier(BLOCK_RANGE_COLUMN)?;
        out.push_sql("), ");
        out.push_bind_param::<Integer, _>(&self.block)?;
        out.push_sql(")");
        Ok(())
    }

    /// Output an expression that matches all rows that have been changed
    /// after `block` (inclusive)
    pub(crate) fn changed_since<'b>(&'b self, out: &mut AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        if self.is_immutable() {
            out.push_identifier(BLOCK_COLUMN)?;
        } else {
            out.push_sql("lower(");
            out.push_identifier(BLOCK_RANGE_COLUMN)?;
            out.push_sql(")");
        }

        out.push_sql(" >= ");
        out.push_bind_param::<Integer, _>(&self.block)?;
        Ok(())
    }
}

#[test]
fn block_number_max_is_i32_max() {
    // The code in this file embeds i32::MAX aka BLOCK_NUMBER_MAX in strings
    // for efficiency. This assertion makes sure that BLOCK_NUMBER_MAX still
    // is what we think it is
    assert_eq!(2147483647, BLOCK_NUMBER_MAX);
}
