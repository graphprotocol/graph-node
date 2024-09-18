use async_graphql::Enum;

/// Types of commands that run in the background.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(remote = "graphman_store::CommandKind")]
pub enum CommandKind {
    RestartDeployment,
}
