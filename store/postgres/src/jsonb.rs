use diesel::expression::helper_types::AsExprOf;
use diesel::expression::{AsExpression, Expression};
use diesel::sql_types::Jsonb;

mod operators {
    use diesel::sql_types::Jsonb;

    // restrict to backend: Pg
    diesel_infix_operator!(JsonbMerge, " || ", Jsonb, backend: diesel::pg::Pg);
}

pub type JsonbMerge<Lhs, Rhs> = operators::JsonbMerge<Lhs, AsExprOf<Rhs, Jsonb>>;

pub trait PgJsonbExpressionMethods: Expression<SqlType = Jsonb> + Sized {
    fn merge<T: AsExpression<Jsonb>>(self, other: T) -> JsonbMerge<Self, T::Expression> {
        JsonbMerge::<Self, T::Expression>::new(self, other.as_expression())
    }
}

impl<T: Expression<SqlType = Jsonb>> PgJsonbExpressionMethods for T where
    T: Expression<SqlType = Jsonb>
{
}
