use crate::graphman_primitives::{GraphmanLayer, IntuitiveLayering};

/// Used to automatically implement layering for commands
/// that will execute layers in the order they are defined.
pub trait ExtensibleGraphmanCommand {}

impl<C, L> GraphmanLayer<L> for C
where
    C: ExtensibleGraphmanCommand,
{
    type Outer = IntuitiveLayering<C, (L,)>;

    fn layer(self, layer: L) -> Self::Outer {
        IntuitiveLayering::new(self).layer(layer)
    }
}
