//! Tools for parsing SQL expressions
use sqlparser::ast as p;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::{Parser as SqlParser, ParserError};
use sqlparser::tokenizer::Tokenizer;

use crate::schema::SchemaValidationError;

pub(crate) trait CheckIdentFn: Fn(&str) -> Result<(), SchemaValidationError> {}

impl<T> CheckIdentFn for T where T: Fn(&str) -> Result<(), SchemaValidationError> {}

/// Parse a SQL expression and check that it only uses whitelisted
/// operations and functions. The `check_ident` function is called for each
/// identifier in the expression
pub(crate) fn parse<F: CheckIdentFn>(
    sql: &str,
    check_ident: F,
) -> Result<(), Vec<SchemaValidationError>> {
    let mut validator = Validator {
        check_ident,
        errors: Vec::new(),
    };
    VisitExpr::visit(sql, &mut validator)
        .map(|_| ())
        .map_err(|()| validator.errors)
}

/// A visitor for `VistExpr` that gets called for the constructs for which
/// we need different behavior between validation and query generation in
/// `store/postgres/src/relational/rollup.rs`. Note that the visitor can
/// mutate both itself (e.g., to store errors) and the expression it is
/// visiting.
pub trait ExprVisitor {
    /// Visit an identifier (column name). Must return `Err` if the
    /// identifier is not allowed
    fn visit_ident(&mut self, ident: &mut p::Ident) -> Result<(), ()>;
    /// Visit a function name. Must return `Err` if the function is not
    /// allowed
    fn visit_func_name(&mut self, func: &mut p::Ident) -> Result<(), ()>;
    /// Called when we encounter a construct that is not supported like a
    /// subquery
    fn not_supported(&mut self, msg: String);
    /// Called if the SQL expression we are visiting has SQL syntax errors
    fn parse_error(&mut self, e: sqlparser::parser::ParserError);
}

pub struct VisitExpr<'a> {
    visitor: Box<&'a mut dyn ExprVisitor>,
}

impl<'a> VisitExpr<'a> {
    fn nope(&mut self, construct: &str) -> Result<(), ()> {
        self.not_supported(format!("Expressions using {construct} are not supported"))
    }

    fn illegal_function(&mut self, msg: String) -> Result<(), ()> {
        self.not_supported(format!("Illegal function: {msg}"))
    }

    fn not_supported(&mut self, msg: String) -> Result<(), ()> {
        self.visitor.not_supported(msg);
        Err(())
    }

    /// Parse `sql` into an expression and traverse it, calling back into
    /// `visitor` at the appropriate places. Return the parsed expression,
    /// which might have been changed by the visitor, on success. On error,
    /// return `Err(())`. The visitor will know the details of the error
    /// since this can only happen if `visit_ident` or `visit_func_name`
    /// returned an error, or `parse_error` or `not_supported` was called.
    pub fn visit(sql: &str, visitor: &'a mut dyn ExprVisitor) -> Result<p::Expr, ()> {
        let dialect = PostgreSqlDialect {};

        let mut parser = SqlParser::new(&dialect);
        let tokens = Tokenizer::new(&dialect, sql)
            .with_unescape(true)
            .tokenize_with_location()
            .unwrap();
        parser = parser.with_tokens_with_locations(tokens);
        let mut visit = VisitExpr {
            visitor: Box::new(visitor),
        };
        let mut expr = match parser.parse_expr() {
            Ok(expr) => expr,
            Err(e) => {
                visitor.parse_error(e);
                return Err(());
            }
        };
        visit.visit_expr(&mut expr).map(|()| expr)
    }

    fn visit_expr(&mut self, expr: &mut p::Expr) -> Result<(), ()> {
        use p::Expr::*;

        match expr {
            Identifier(ident) => self.visitor.visit_ident(ident),
            BinaryOp { left, op, right } => {
                self.check_binary_op(op)?;
                self.visit_expr(left)?;
                self.visit_expr(right)?;
                Ok(())
            }
            UnaryOp { op, expr } => {
                self.check_unary_op(op)?;
                self.visit_expr(expr)?;
                Ok(())
            }
            Function(func) => self.visit_func(func),
            Value(_) => Ok(()),
            Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(operand) = operand {
                    self.visit_expr(operand)?;
                }
                for condition in conditions {
                    self.visit_expr(condition)?;
                }
                for result in results {
                    self.visit_expr(result)?;
                }
                if let Some(else_result) = else_result {
                    self.visit_expr(else_result)?;
                }
                Ok(())
            }
            Cast {
                expr,
                data_type: _,
                kind,
                format: _,
            } => match kind {
                // Cast: `CAST(<expr> as <datatype>)`
                // DoubleColon: `<expr>::<datatype>`
                p::CastKind::Cast | p::CastKind::DoubleColon => self.visit_expr(expr),
                // These two are not Postgres syntax
                p::CastKind::TryCast | p::CastKind::SafeCast => {
                    self.nope(&format!("non-standard cast '{:?}'", kind))
                }
            },
            Nested(expr) | IsFalse(expr) | IsNotFalse(expr) | IsTrue(expr) | IsNotTrue(expr)
            | IsNull(expr) | IsNotNull(expr) => self.visit_expr(expr),
            IsDistinctFrom(expr1, expr2) | IsNotDistinctFrom(expr1, expr2) => {
                self.visit_expr(expr1)?;
                self.visit_expr(expr2)?;
                Ok(())
            }
            CompoundIdentifier(_) => self.nope("CompoundIdentifier"),
            JsonAccess { .. } => self.nope("JsonAccess"),
            CompositeAccess { .. } => self.nope("CompositeAccess"),
            IsUnknown(_) => self.nope("IsUnknown"),
            IsNotUnknown(_) => self.nope("IsNotUnknown"),
            InList { .. } => self.nope("InList"),
            InSubquery { .. } => self.nope("InSubquery"),
            InUnnest { .. } => self.nope("InUnnest"),
            Between { .. } => self.nope("Between"),
            Like { .. } => self.nope("Like"),
            ILike { .. } => self.nope("ILike"),
            SimilarTo { .. } => self.nope("SimilarTo"),
            RLike { .. } => self.nope("RLike"),
            AnyOp { .. } => self.nope("AnyOp"),
            AllOp { .. } => self.nope("AllOp"),
            Convert { .. } => self.nope("Convert"),
            AtTimeZone { .. } => self.nope("AtTimeZone"),
            Extract { .. } => self.nope("Extract"),
            Ceil { .. } => self.nope("Ceil"),
            Floor { .. } => self.nope("Floor"),
            Position { .. } => self.nope("Position"),
            Substring { .. } => self.nope("Substring"),
            Trim { .. } => self.nope("Trim"),
            Overlay { .. } => self.nope("Overlay"),
            Collate { .. } => self.nope("Collate"),
            IntroducedString { .. } => self.nope("IntroducedString"),
            TypedString { .. } => self.nope("TypedString"),
            MapAccess { .. } => self.nope("MapAccess"),
            Exists { .. } => self.nope("Exists"),
            Subquery(_) => self.nope("Subquery"),
            GroupingSets(_) => self.nope("GroupingSets"),
            Cube(_) => self.nope("Cube"),
            Rollup(_) => self.nope("Rollup"),
            Tuple(_) => self.nope("Tuple"),
            Struct { .. } => self.nope("Struct"),
            Named { .. } => self.nope("Named"),
            ArrayIndex { .. } => self.nope("ArrayIndex"),
            Array(_) => self.nope("Array"),
            Interval(_) => self.nope("Interval"),
            MatchAgainst { .. } => self.nope("MatchAgainst"),
            Wildcard => self.nope("Wildcard"),
            QualifiedWildcard(_) => self.nope("QualifiedWildcard"),
            Dictionary(_) => self.nope("Dictionary"),
            OuterJoin(_) => self.nope("OuterJoin"),
            Prior(_) => self.nope("Prior"),
        }
    }

    fn visit_func(&mut self, func: &mut p::Function) -> Result<(), ()> {
        let p::Function {
            name,
            args: pargs,
            filter,
            null_treatment,
            over,
            within_group,
        } = func;

        if filter.is_some()
            || null_treatment.is_some()
            || over.is_some()
            || !within_group.is_empty()
        {
            return self.illegal_function(format!("call to {name} uses an illegal feature"));
        }

        let idents = &mut name.0;
        if idents.len() != 1 {
            return self.illegal_function(format!(
                "function name {name} uses a qualified name with '.'"
            ));
        }
        self.visitor.visit_func_name(&mut idents[0])?;
        match pargs {
            p::FunctionArguments::None => { /* nothing to do */ }
            p::FunctionArguments::Subquery(_) => {
                return self.illegal_function(format!("call to {name} uses a subquery argument"))
            }
            p::FunctionArguments::List(pargs) => {
                let p::FunctionArgumentList {
                    duplicate_treatment,
                    args,
                    clauses,
                } = pargs;
                if duplicate_treatment.is_some() {
                    return self
                        .illegal_function(format!("call to {name} uses a duplicate treatment"));
                }
                if !clauses.is_empty() {
                    return self.illegal_function(format!("call to {name} uses a clause"));
                }
                for arg in args {
                    use p::FunctionArg::*;
                    match arg {
                        Named { .. } => {
                            return self
                                .illegal_function(format!("call to {name} uses a named argument"));
                        }
                        Unnamed(arg) => match arg {
                            p::FunctionArgExpr::Expr(expr) => {
                                self.visit_expr(expr)?;
                            }
                            p::FunctionArgExpr::QualifiedWildcard(_)
                            | p::FunctionArgExpr::Wildcard => {
                                return self.illegal_function(format!(
                                    "call to {name} uses a wildcard argument"
                                ));
                            }
                        },
                    };
                }
            }
        }
        Ok(())
    }

    fn check_binary_op(&mut self, op: &p::BinaryOperator) -> Result<(), ()> {
        use p::BinaryOperator::*;
        match op {
            Plus | Minus | Multiply | Divide | Modulo | PGExp | Gt | Lt | GtEq | LtEq
            | Spaceship | Eq | NotEq | And | Or => Ok(()),
            StringConcat
            | Xor
            | BitwiseOr
            | BitwiseAnd
            | BitwiseXor
            | DuckIntegerDivide
            | MyIntegerDivide
            | Custom(_)
            | PGBitwiseXor
            | PGBitwiseShiftLeft
            | PGBitwiseShiftRight
            | PGOverlap
            | PGRegexMatch
            | PGRegexIMatch
            | PGRegexNotMatch
            | PGRegexNotIMatch
            | PGLikeMatch
            | PGILikeMatch
            | PGNotLikeMatch
            | PGNotILikeMatch
            | PGStartsWith
            | PGCustomBinaryOperator(_)
            | Arrow
            | LongArrow
            | HashArrow
            | HashLongArrow
            | AtAt
            | AtArrow
            | ArrowAt
            | HashMinus
            | AtQuestion
            | Question
            | QuestionAnd
            | QuestionPipe => self.not_supported(format!("binary operator {op} is not supported")),
        }
    }

    fn check_unary_op(&mut self, op: &p::UnaryOperator) -> Result<(), ()> {
        use p::UnaryOperator::*;
        match op {
            Plus | Minus | Not => Ok(()),
            PGBitwiseNot | PGSquareRoot | PGCubeRoot | PGPostfixFactorial | PGPrefixFactorial
            | PGAbs => self.not_supported(format!("unary operator {op} is not supported")),
        }
    }
}

/// An `ExprVisitor` that validates an expression
struct Validator<F> {
    check_ident: F,
    errors: Vec<SchemaValidationError>,
}

const FN_WHITELIST: [&'static str; 14] = [
    // Clearly deterministic functions from
    // https://www.postgresql.org/docs/current/functions-math.html, Table
    // 9.5. We could also add trig functions (Table 9.7 and 9.8), but under
    // no circumstances random functions from Table 9.6
    "abs", "ceil", "ceiling", "div", "floor", "gcd", "lcm", "mod", "power", "sign",
    // Conditional functions from
    // https://www.postgresql.org/docs/current/functions-conditional.html.
    "coalesce", "nullif", "greatest", "least",
];

impl<F: CheckIdentFn> ExprVisitor for Validator<F> {
    fn visit_ident(&mut self, ident: &mut p::Ident) -> Result<(), ()> {
        match (self.check_ident)(&ident.value) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.errors.push(e);
                Err(())
            }
        }
    }

    fn visit_func_name(&mut self, func: &mut p::Ident) -> Result<(), ()> {
        let p::Ident { value, quote_style } = &func;
        let whitelisted = match quote_style {
            Some(_) => FN_WHITELIST.contains(&value.as_str()),
            None => FN_WHITELIST
                .iter()
                .any(|name| name.eq_ignore_ascii_case(value)),
        };
        if whitelisted {
            Ok(())
        } else {
            self.not_supported(format!("Function {func} is not supported"));
            Err(())
        }
    }

    fn not_supported(&mut self, msg: String) {
        self.errors
            .push(SchemaValidationError::ExprNotSupported(msg));
    }

    fn parse_error(&mut self, e: ParserError) {
        self.errors
            .push(SchemaValidationError::ExprParseError(e.to_string()));
    }
}
