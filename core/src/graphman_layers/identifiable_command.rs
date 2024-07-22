use uuid::Uuid;

use crate::graphman_primitives::ExtensibleGraphmanContext;
use crate::graphman_primitives::GraphmanCommand;
use crate::graphman_primitives::GraphmanLayer;

pub struct CommandKind(pub &'static str);

pub struct CommandExecutionId(pub Uuid);

/// Makes a command identifiable by assigning it a kind
/// and a unique command execution ID.
pub struct IdentifiableCommand {
    kind: CommandKind,
    execution_id: CommandExecutionId,
}

pub struct IdentifiableCommandLayer<C> {
    kind: CommandKind,
    execution_id: CommandExecutionId,
    inner: C,
}

impl IdentifiableCommand {
    pub fn new(kind: &'static str) -> Self {
        Self {
            kind: CommandKind(kind),
            execution_id: CommandExecutionId(Uuid::new_v4()),
        }
    }
}

impl<C> GraphmanLayer<C> for IdentifiableCommand {
    type Outer = IdentifiableCommandLayer<C>;

    fn layer(self, inner: C) -> Self::Outer {
        let Self { kind, execution_id } = self;

        IdentifiableCommandLayer {
            kind,
            execution_id,
            inner,
        }
    }
}

impl<C, Ctx> GraphmanCommand<Ctx> for IdentifiableCommandLayer<C>
where
    C: GraphmanCommand<Ctx> + Send + 'static,
    Ctx: ExtensibleGraphmanContext + Send + 'static,
{
    type Output = C::Output;
    type Error = C::Error;
    type Future = C::Future;

    fn execute(self, mut ctx: Ctx) -> Self::Future {
        let Self {
            kind,
            execution_id,
            inner,
        } = self;

        ctx.extend(kind);
        ctx.extend(execution_id);

        inner.execute(ctx)
    }
}
