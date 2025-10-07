//! Helpers for working with async Postgres connections.

use diesel::{connection::TransactionManager, result::Error, Connection};
use diesel_async::scoped_futures::ScopedBoxFuture;
use graph::futures03::{future::BoxFuture, FutureExt};

use crate::pool::PgConnection;

/// A diesel connection that supports async transactions that can be used
/// with synchronous connections. This implementation is blocking even if
/// `callback` is not  because starting, committing, and rolling back the
/// transaction is done synchronously just like `Connection.transaction`
pub trait AsyncConnection: Connection {
    fn transaction_async<'a, 'conn, R, E, F>(
        &'conn mut self,
        callback: F,
    ) -> BoxFuture<'conn, Result<R, E>>
    where
        F: for<'r> FnOnce(&'r mut Self) -> ScopedBoxFuture<'a, 'r, Result<R, E>> + Send + 'a,
        E: From<Error> + Send + 'a,
        R: Send + 'a,
        'a: 'conn,
    {
        async move {
            Self::TransactionManager::begin_transaction(self)?;
            match callback(&mut *self).await {
                Ok(value) => {
                    Self::TransactionManager::commit_transaction(self)?;
                    Ok(value)
                }
                Err(user_error) => match Self::TransactionManager::rollback_transaction(self) {
                    Ok(()) => Err(user_error),
                    Err(Error::BrokenTransactionManager) => {
                        // In this case we are probably more interested by the
                        // original error, which likely caused this
                        Err(user_error)
                    }
                    Err(rollback_error) => Err(rollback_error.into()),
                },
            }
        }
        .boxed()
    }
}

impl AsyncConnection for PgConnection {}
