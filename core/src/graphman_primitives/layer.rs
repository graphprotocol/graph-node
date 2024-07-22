/// Makes it possible to add functionality to commands by layering
/// new functionality on top of existing functionality.
pub trait GraphmanLayer<C> {
    type Outer;

    /// Extends a command or a layer with additional functionality.
    fn layer(self, inner: C) -> Self::Outer;
}
