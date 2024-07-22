use crate::graphman_primitives::GraphmanCommand;
use crate::graphman_primitives::GraphmanLayer;

/// Allows a command to be extended with layers (additional functionality)
/// that are executed in the order of their appearance.
pub struct IntuitiveLayering<C, L> {
    inner: C,
    layers: L,
}

impl<C> IntuitiveLayering<C, ()> {
    pub fn new(inner: C) -> Self {
        Self { inner, layers: () }
    }

    /// Extends the command with additional functionality.
    pub fn layer<L>(self, layer: L) -> IntuitiveLayering<C, (L,)> {
        IntuitiveLayering {
            inner: self.inner,
            layers: (layer,),
        }
    }
}

// A helper macro that generates impl blocks for different numbers of layers.
macro_rules! impl_intuitive_layering {
    ($( $lx:ident ),+) => {
        impl_intuitive_layering!(@layer $( $lx, )+);
        impl_intuitive_layering!(@command $( $lx, )+);
    };

    (@layer $( $lx:ident ),+ $(,)*) => {
        #[allow(non_snake_case)]
        impl<C, $( $lx, )+> IntuitiveLayering<C, ($( $lx, )+)> {
            /// Extends the command with additional functionality.
            pub fn layer<L>(self, layer: L) -> IntuitiveLayering<C, (L, $( $lx, )+)> {
                let ($( $lx, )+) = self.layers;

                IntuitiveLayering {
                    inner: self.inner,
                    layers: (layer, $( $lx, )+),
                }
            }
        }
    };

    (@command $( $lx:ident ),+ $(,)*) => {
        impl_intuitive_layering!(@command $( $lx, )+ ; $( $lx, )+ ;);
    };

    (@command $( $lx:ident ),+ $(,)* ; $l1:ident, $( $lx_wo_l1:ident ),+ $(,)* ; $( $lx_wo_ln:ident ),* $(,)*) => {
        impl_intuitive_layering!(@command $( $lx, )+ ; $( $lx_wo_l1, )+ ; $( $lx_wo_ln, )* $l1);
    };

    (@command $( $lx:ident ),+ $(,)* ; $ln:ident $(,)* ; $( $lx_wo_ln:ident ),* $(,)*) => {
        impl_intuitive_layering!(@command $( $lx, )+ ; ; $( $lx_wo_ln, )* ; $ln);
    };

    (@command $l1:ident, $( $lx_wo_l1:ident ),* $(,)* ; ; $( $lx_wo_ln:ident ),* $(,)* ; $ln:ident) => {
        #[allow(non_snake_case)]
        impl<C, Ctx, $l1, $( $lx_wo_l1, )*> GraphmanCommand<Ctx> for IntuitiveLayering<C, ($l1, $( $lx_wo_l1, )*)>
        where
            $l1: GraphmanLayer<C>,
            $(
                $lx_wo_l1: GraphmanLayer<$lx_wo_ln::Outer>,
            )*
            $ln::Outer: GraphmanCommand<Ctx>
        {
            type Output = <$ln::Outer as GraphmanCommand<Ctx>>::Output;
            type Error = <$ln::Outer as GraphmanCommand<Ctx>>::Error;
            type Future = <$ln::Outer as GraphmanCommand<Ctx>>::Future;

            fn execute(self, ctx: Ctx) -> Self::Future {
                let ($l1, $( $lx_wo_l1, )*) = self.layers;

                let output = $l1.layer(self.inner);
                $( let output = $lx_wo_l1.layer(output); )*

                output.execute(ctx)
            }
        }
    };
}

impl_intuitive_layering!(L1);
impl_intuitive_layering!(L1, L2);
impl_intuitive_layering!(L1, L2, L3);
impl_intuitive_layering!(L1, L2, L3, L4);
impl_intuitive_layering!(L1, L2, L3, L4, L5);
impl_intuitive_layering!(L1, L2, L3, L4, L5, L6);
impl_intuitive_layering!(L1, L2, L3, L4, L5, L6, L7);
impl_intuitive_layering!(L1, L2, L3, L4, L5, L6, L7, L8);
impl_intuitive_layering!(L1, L2, L3, L4, L5, L6, L7, L8, L9);
impl_intuitive_layering!(@command L1, L2, L3, L4, L5, L6, L7, L8, L9, L10);

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use crate::graphman_primitives::BoxedFuture;

    use super::*;

    struct TestCommand(&'static str);

    struct TestExtension(&'static str);

    struct TestExtensionLayer<C>(&'static str, C);

    impl<Ctx> GraphmanCommand<Ctx> for TestCommand {
        type Output = String;
        type Error = Infallible;
        type Future = BoxedFuture<Self::Output, Self::Error>;

        fn execute(self, _ctx: Ctx) -> Self::Future {
            Box::pin(async move { Ok(self.0.to_owned()) })
        }
    }

    impl<C> GraphmanLayer<C> for TestExtension {
        type Outer = TestExtensionLayer<C>;

        fn layer(self, inner: C) -> Self::Outer {
            TestExtensionLayer(self.0, inner)
        }
    }

    impl<C, Ctx> GraphmanCommand<Ctx> for TestExtensionLayer<C>
    where
        C: GraphmanCommand<Ctx> + Send + 'static,
        <C as GraphmanCommand<Ctx>>::Output: std::fmt::Display,
        <C as GraphmanCommand<Ctx>>::Error: std::fmt::Debug,
        Ctx: Send + 'static,
    {
        type Output = String;
        type Error = Infallible;
        type Future = BoxedFuture<Self::Output, Self::Error>;

        fn execute(self, ctx: Ctx) -> Self::Future {
            Box::pin(async move {
                let inner_output = self.1.execute(ctx).await.unwrap();
                Ok(format!("{},{}", self.0, inner_output))
            })
        }
    }

    #[tokio::test]
    async fn intuitive_layering_with_one_layer() {
        let command = IntuitiveLayering::new(TestCommand("A")).layer(TestExtension("B"));

        let output = command.execute(()).await.unwrap();

        assert_eq!(output, "B,A");
    }

    #[tokio::test]
    async fn intuitive_layering_with_two_layers() {
        let command = IntuitiveLayering::new(TestCommand("A"))
            .layer(TestExtension("B"))
            .layer(TestExtension("C"));

        let output = command.execute(()).await.unwrap();

        assert_eq!(output, "B,C,A");
    }

    #[tokio::test]
    async fn intuitive_layering_with_three_layers() {
        let command = IntuitiveLayering::new(TestCommand("A"))
            .layer(TestExtension("B"))
            .layer(TestExtension("C"))
            .layer(TestExtension("D"));

        let output = command.execute(()).await.unwrap();

        assert_eq!(output, "B,C,D,A");
    }

    #[tokio::test]
    async fn intuitive_layering_with_four_layers() {
        let command = IntuitiveLayering::new(TestCommand("A"))
            .layer(TestExtension("B"))
            .layer(TestExtension("C"))
            .layer(TestExtension("D"))
            .layer(TestExtension("E"));

        let output = command.execute(()).await.unwrap();

        assert_eq!(output, "B,C,D,E,A");
    }

    #[tokio::test]
    async fn intuitive_layering_with_five_layers() {
        let command = IntuitiveLayering::new(TestCommand("A"))
            .layer(TestExtension("B"))
            .layer(TestExtension("C"))
            .layer(TestExtension("D"))
            .layer(TestExtension("E"))
            .layer(TestExtension("F"));

        let output = command.execute(()).await.unwrap();

        assert_eq!(output, "B,C,D,E,F,A");
    }

    #[tokio::test]
    async fn intuitive_layering_with_six_layers() {
        let command = IntuitiveLayering::new(TestCommand("A"))
            .layer(TestExtension("B"))
            .layer(TestExtension("C"))
            .layer(TestExtension("D"))
            .layer(TestExtension("E"))
            .layer(TestExtension("F"))
            .layer(TestExtension("G"));

        let output = command.execute(()).await.unwrap();

        assert_eq!(output, "B,C,D,E,F,G,A");
    }

    #[tokio::test]
    async fn intuitive_layering_with_seven_layers() {
        let command = IntuitiveLayering::new(TestCommand("A"))
            .layer(TestExtension("B"))
            .layer(TestExtension("C"))
            .layer(TestExtension("D"))
            .layer(TestExtension("E"))
            .layer(TestExtension("F"))
            .layer(TestExtension("G"))
            .layer(TestExtension("H"));

        let output = command.execute(()).await.unwrap();

        assert_eq!(output, "B,C,D,E,F,G,H,A");
    }

    #[tokio::test]
    async fn intuitive_layering_with_eight_layers() {
        let command = IntuitiveLayering::new(TestCommand("A"))
            .layer(TestExtension("B"))
            .layer(TestExtension("C"))
            .layer(TestExtension("D"))
            .layer(TestExtension("E"))
            .layer(TestExtension("F"))
            .layer(TestExtension("G"))
            .layer(TestExtension("H"))
            .layer(TestExtension("I"));

        let output = command.execute(()).await.unwrap();

        assert_eq!(output, "B,C,D,E,F,G,H,I,A");
    }

    #[tokio::test]
    async fn intuitive_layering_with_nine_layers() {
        let command = IntuitiveLayering::new(TestCommand("A"))
            .layer(TestExtension("B"))
            .layer(TestExtension("C"))
            .layer(TestExtension("D"))
            .layer(TestExtension("E"))
            .layer(TestExtension("F"))
            .layer(TestExtension("G"))
            .layer(TestExtension("H"))
            .layer(TestExtension("I"))
            .layer(TestExtension("J"));

        let output = command.execute(()).await.unwrap();

        assert_eq!(output, "B,C,D,E,F,G,H,I,J,A");
    }

    #[tokio::test]
    async fn intuitive_layering_with_ten_layers() {
        let command = IntuitiveLayering::new(TestCommand("A"))
            .layer(TestExtension("B"))
            .layer(TestExtension("C"))
            .layer(TestExtension("D"))
            .layer(TestExtension("E"))
            .layer(TestExtension("F"))
            .layer(TestExtension("G"))
            .layer(TestExtension("H"))
            .layer(TestExtension("I"))
            .layer(TestExtension("J"))
            .layer(TestExtension("K"));

        let output = command.execute(()).await.unwrap();

        assert_eq!(output, "B,C,D,E,F,G,H,I,J,K,A");
    }
}
