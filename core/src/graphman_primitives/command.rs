use std::future::Future;
use std::pin::Pin;

/// Used on commands and layers when implementing [GraphmanCommand]
/// to make the associated type shorter.
pub type BoxedFuture<Output, Error> =
    Pin<Box<dyn Future<Output = Result<Output, Error>> + Send + 'static>>;

/// Describes any command or layer which can be executed
/// and produce either an output or an error.
///
/// Useful when extending commands with additional functionality such as:
/// assigning unique IDs, running commands in the background, or tracking command execution.
pub trait GraphmanCommand<Ctx> {
    type Output;
    type Error;
    type Future: Future<Output = Result<Self::Output, Self::Error>> + Send + 'static;

    fn execute(self, ctx: Ctx) -> Self::Future;
}
