use std::fmt;
use std::marker::PhantomData;

/// This struct is intended to prevent deadlocks that can occur when
/// acquiring a pooled db connection. A deadlock can occur whenever
/// a task attempts to acquire a pooled connection while already holding one.
/// Consider the case for a pooled connection and a pool size of 2:
///
/// fn() deadlock() {
///    let conn1 = acquire();
///    sleep(1000);
///    let conn2 = acquire();
/// }
///
/// If the above is called by two threads at the same time, both will
/// acquire conn1, but will not be able to acquire conn2 because
/// the pool is empty and neither thread can progress far enough to drop
/// conn1 leaving them both stuck.
///
/// The case above is relatively easy to monitor, but when the code
/// gets complex this is harder to detect. Consider for instance
/// if conn2 was acquired deep in the call stack through several
/// indirect function calls or even in other tasks.
///
/// TODO: Explain how this fixes the problem
pub struct DbAccess {
    _private: (),
}

impl DbAccess {
    // TODO: Document invariants for calling.
    // (Eg: Top level task)
    pub unsafe fn new() -> Self {
        Self { _private: () }
    }

    pub fn wrap<'a, T>(&'a mut self, value: T) -> Accessed<'a, T> {
        Accessed {
            _access: PhantomData,
            value,
        }
    }
}

pub struct Accessed<'a, T> {
    value: T,
    // Using a PhantomData here ensures this is a transparent wrapper
    _access: PhantomData<&'a ()>,
}

impl fmt::Debug for DbAccess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DbAccess {{}}")
    }
}

impl<'a, T> fmt::Debug for Accessed<'a, T>
where
    T: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value.fmt(f)
    }
}

impl<'a, T> fmt::Display for Accessed<'a, T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value.fmt(f)
    }
}

impl<'a, T> std::error::Error for Accessed<'a, T> where T: std::error::Error {}

impl<'a, T> std::ops::Deref for Accessed<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}
