/// A value that can be extended with an instance of the same type.
pub trait Extend<T> {
    fn extend(self, other: T) -> Self;
}
