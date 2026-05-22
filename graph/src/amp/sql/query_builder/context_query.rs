use itertools::Itertools;
use sqlparser_latest::ast;

use super::parse_query;

/// Wraps the SQL query with additional context columns from a separate dataset.
///
/// Creates two CTEs: one wrapping the input `query` and another loading context columns
/// from the specified context dataset and table. Joins both CTEs on block numbers to
/// include the context columns in the original query's output.
///
/// This enables including columns required by Amp subgraphs in the original SQL query.
pub(super) fn new_context_query<'a>(
    query: &ast::Query,
    block_number_column: &str,
    context_dataset: &str,
    context_table: &str,
    context_columns: impl IntoIterator<Item = &'a str>,
) -> ast::Query {
    // CTE names are unique within a SQL query.
    let context_columns = context_columns.into_iter().collect_vec();
    assert!(!context_columns.is_empty());

    let context_cte = "amp_ctx";
    let source_cte = "amp_src";

    let context_query = format!(
        "
        WITH {context_cte} AS (
            SELECT DISTINCT _block_num, {input_context_columns} FROM {context_dataset}.{context_table}
        ),
        {source_cte} AS (
            {query}
        )
        SELECT
            {output_context_columns},
            {source_cte}.*
        FROM
            {source_cte}
        INNER JOIN {context_cte} ON
            {context_cte}._block_num = {source_cte}.{block_number_column}
        ",
        input_context_columns = context_columns.join(", "),
        output_context_columns = context_columns
            .iter()
            .map(|context_column| format!("{context_cte}.{context_column}"))
            .join(", "),
    );

    parse_query(context_query).unwrap()
}

#[cfg(test)]
mod tests {
    use super::super::parse_query;
    use super::*;

    #[test]
    fn query_is_wrapped_with_context() {
        let query = parse_query("SELECT a, b, c FROM d").unwrap();
        let block_number_column = "b";
        let context_dataset = "cx_a";
        let context_table = "cx_b";
        let context_columns = ["cx_c", "cx_d"];

        let context_query = new_context_query(
            &query,
            block_number_column,
            context_dataset,
            context_table,
            context_columns,
        );

        assert_eq!(
            context_query,
            parse_query(
                "
                WITH amp_ctx AS (
                       SELECT DISTINCT _block_num, cx_c, cx_d FROM cx_a.cx_b
                     ),
                     amp_src AS (SELECT a, b, c FROM d)
              SELECT amp_ctx.cx_c, amp_ctx.cx_d, amp_src.*
                FROM amp_src
                INNER JOIN amp_ctx ON amp_ctx._block_num = amp_src.b"
            )
            .unwrap()
        )
    }
}
