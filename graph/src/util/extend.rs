/// A value that can be extended with an instance of the same type.
pub trait Extend<T> {
    fn extend(self, other: T) -> Self;
}

/// An optional value that can be extended with an instance of its inner type.
impl<T> Extend<T> for Option<T>
where
    T: Extend<T>,
{
    fn extend(self, other: T) -> Self {
        match self {
            None => Some(other),
            Some(inner) => Some(inner.extend(other)),
        }
    }
}
