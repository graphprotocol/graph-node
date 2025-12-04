//! This module is responsible for executing subgraph runner futures.
//!
//! # Terminology used in this module
//!
//! `active subgraph` - A subgraph that was started and is still kept in memory in the list of started subgraphs.
//! `running subgraph` - A subgraph that has an instance that is making progress or stopping.
//! `subgraph instance` - A background task that executes the subgraph runner future.

use std::{
    collections::{hash_map::Entry, HashMap},
    fmt,
    sync::{
        atomic::{AtomicU32, Ordering::SeqCst},
        Arc,
    },
    time::Duration,
};

use anyhow::Result;
use futures::future::BoxFuture;
use graph::{
    cheap_clone::CheapClone, components::store::DeploymentLocator, log::factory::LoggerFactory,
};
use slog::{debug, error, info, warn, Logger};
use tokio::{sync::mpsc, task::JoinHandle, time::timeout};
use tokio_util::sync::CancellationToken;

/// Represents the maximum amount of time a subgraph instance is allowed to run
/// after it receives a cancel signal.
///
/// If a subgraph instance does not complete its execution in this amount of time
/// it is considered unresponsive and is aborted.
const SUBGRAPH_INSTANCE_GRACE_PERIOD: Duration = {
    if cfg!(test) {
        Duration::from_millis(300)
    } else if cfg!(debug_assertions) {
        Duration::from_secs(30)
    } else {
        Duration::from_secs(300)
    }
};

/// Represents the subgraph runner future.
///
/// This is the future that performs the subgraph indexing.
/// It is expected to return only on deterministic failures or when indexing is completed.
/// All retry functionality must be handled internally by this future.
pub(super) type BoxRunner =
    Box<dyn FnOnce(CancellationToken) -> BoxFuture<'static, Result<()>> + Send + 'static>;

/// Manages the lifecycle of subgraph runners.
///
/// Ensures that there is at most one subgraph instance running
/// for any subgraph deployment at any point in time.
/// Handles starting, stopping and restarting subgraphs.
pub(super) struct Monitor {
    logger_factory: Arc<LoggerFactory>,

    /// Every subgraph instance is assigned a cancel token derived from this token.
    ///
    /// This means that the `Monitor` can send cancel signals to all subgraph instances at once,
    /// and to each subgraph instance individually.
    cancel_token: CancellationToken,

    /// The channel that is used to send subgraph commands.
    ///
    /// Every subgraph start and stop request results in a command that is sent to the
    /// background task that manages the subgraph instances.
    command_tx: mpsc::UnboundedSender<Command>,

    /// When a subgraph starts it is assigned a sequential ID.
    /// The ID is then kept in memory in the list of active subgraphs.
    ///
    /// When the subgraph completes execution it should be removed from the
    /// list of active subgraphs, so that it can be restarted.
    ///
    /// This ID is required to be able to check if the active subgraph
    /// is the same subgraph instance that was stopped.
    ///
    /// If the IDs do not match, it means that the subgraph was force restarted,
    /// ignoring the state of the previous subgraph instance, or that the subgraph
    /// was restarted after the previous subgraph instance completed its execution
    /// but before the remove request was processed.
    subgraph_instance_id: Arc<AtomicU32>,
}

impl Monitor {
    /// Creates a new subgraph monitor.
    ///
    /// Spawns a background task that manages the subgraph start and stop requests.
    ///
    /// A new cancel token is derived from the `cancel_token` and only the derived token is used by the
    /// subgraph monitor and its background task.
    pub(super) fn new(logger_factory: &LoggerFactory, cancel_token: &CancellationToken) -> Self {
        let logger = logger_factory.component_logger("AmpSubgraphMonitor", None);
        let logger_factory = Arc::new(logger_factory.with_parent(logger));

        // A derived token makes sure it is not possible to accidentally cancel the parent token
        let cancel_token = cancel_token.child_token();

        // It is safe to use an unbounded channel here, because it's pretty much unrealistic that the
        // command processor will fall behind so much that the channel buffer will take up all the memory.
        // The command processor is non-blocking and delegates long-running processes to detached tasks.
        let (command_tx, command_rx) = mpsc::unbounded_channel::<Command>();

        tokio::spawn(Self::command_processor(
            logger_factory.cheap_clone(),
            cancel_token.cheap_clone(),
            command_tx.clone(),
            command_rx,
        ));

        Self {
            logger_factory,
            cancel_token,
            command_tx,
            subgraph_instance_id: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Starts a subgraph.
    ///
    /// Sends a subgraph start request to this subgraph monitor that
    /// eventually starts the subgraph.
    ///
    /// # Behaviour
    ///
    /// - If the subgraph is not active, it starts when the request is processed
    /// - If the subgraph is active, it stops, and then restarts
    /// - Ensures that there is only one subgraph instance for this subgraph deployment
    /// - Multiple consecutive calls in a short time period force restart the subgraph,
    ///   aborting the active subgraph instance
    pub(super) fn start(&self, deployment: DeploymentLocator, runner: BoxRunner) {
        let logger = self
            .logger_factory
            .subgraph_logger(&deployment)
            .new(slog::o!("method" => "start"));

        info!(logger, "Starting subgraph");
        handle_send_result(
            &logger,
            self.command_tx.send(Command::Start {
                id: self.subgraph_instance_id.fetch_add(1, SeqCst),
                deployment,
                runner,
            }),
        );
    }

    /// Stops the subgraph.
    ///
    /// Sends a subgraph stop request to this subgraph monitor that
    /// eventually stops the subgraph.
    ///
    /// # Behaviour
    ///
    /// - If the subgraph is not active does nothing
    /// - If the subgraph is active, sends a cancel signal that gracefully stops the subgraph
    /// - If the subgraph fails to stop after an extended period of time it aborts
    pub(super) fn stop(&self, deployment: DeploymentLocator) {
        let logger = self
            .logger_factory
            .subgraph_logger(&deployment)
            .new(slog::o!("method" => "stop"));

        info!(logger, "Stopping subgraph");
        handle_send_result(&logger, self.command_tx.send(Command::Stop { deployment }));
    }

    /// Processes commands sent through the command channel.
    ///
    /// Tracks active subgraphs and keeps a list of pending start commands.
    /// Pending start commands are start commands that execute after the related subgraph stops.
    async fn command_processor(
        logger_factory: Arc<LoggerFactory>,
        cancel_token: CancellationToken,
        command_tx: mpsc::UnboundedSender<Command>,
        mut command_rx: mpsc::UnboundedReceiver<Command>,
    ) {
        let logger = logger_factory.component_logger("CommandProcessor", None);
        let mut subgraph_instances: HashMap<DeploymentLocator, SubgraphInstance> = HashMap::new();
        let mut pending_start_commands: HashMap<DeploymentLocator, Command> = HashMap::new();

        loop {
            tokio::select! {
                Some(command) = command_rx.recv() => {
                    debug!(logger, "Processing a new command";
                        "command" => ?command
                    );

                    match &command {
                        Command::Start { .. } => {
                            Self::process_start_command(
                                &logger_factory,
                                &cancel_token,
                                &mut subgraph_instances,
                                &mut pending_start_commands,
                                &command_tx,
                                command
                            );
                        },
                        Command::Stop { .. } => {
                            Self::process_stop_command(
                                &logger_factory,
                                &mut subgraph_instances,
                                &mut pending_start_commands,
                                command
                            );
                        },
                        Command::Clear { .. } => {
                            Self::process_clear_command(
                                &logger_factory,
                                &mut subgraph_instances,
                                &mut pending_start_commands,
                                &command_tx,
                                command
                            );
                        },
                    }
                },
                _ = cancel_token.cancelled() => {
                    debug!(logger, "Stopping command processor");

                    // All active subgraphs will shutdown gracefully
                    // because their cancel tokens are derived from this cancelled token.
                    return;
                }
            }
        }
    }

    /// Starts a subgraph.
    ///
    /// # Behaviour
    ///
    /// - If the subgraph is not active, it starts right away
    /// - If the subgraph is active, a cancel signal is sent to the active subgraph instance
    ///   and this start request is stored in the list of pending start commands
    /// - If the subgraph is active and there is already a pending start command,
    ///   the active subgraph instance aborts, and the subgraph force restarts right away
    /// - If the subgraph is active, but its instance is not actually running,
    ///   the subgraph starts right away
    fn process_start_command(
        logger_factory: &LoggerFactory,
        cancel_token: &CancellationToken,
        subgraph_instances: &mut HashMap<DeploymentLocator, SubgraphInstance>,
        pending_start_commands: &mut HashMap<DeploymentLocator, Command>,
        command_tx: &mpsc::UnboundedSender<Command>,
        command: Command,
    ) {
        let Command::Start {
            id,
            deployment,
            runner,
        } = command
        else {
            unreachable!();
        };

        let logger = logger_factory.subgraph_logger(&deployment);
        let command_logger = logger.new(slog::o!("command" => "start"));

        let cancel_token = cancel_token.child_token();
        let pending_start_command = pending_start_commands.remove(&deployment);

        match subgraph_instances.entry(deployment.cheap_clone()) {
            Entry::Vacant(entry) => {
                debug!(command_logger, "Subgraph is not active, starting");

                let subgraph_instance = Self::start_subgraph(
                    logger,
                    cancel_token,
                    id,
                    deployment,
                    runner,
                    command_tx.clone(),
                );

                entry.insert(subgraph_instance);
            }
            Entry::Occupied(mut entry) => {
                let subgraph_instance = entry.get_mut();
                subgraph_instance.cancel_token.cancel();

                if pending_start_command.is_some() {
                    debug!(command_logger, "Subgraph is active, force restarting");

                    subgraph_instance.handle.abort();

                    *subgraph_instance = Self::start_subgraph(
                        logger,
                        cancel_token,
                        id,
                        deployment,
                        runner,
                        command_tx.clone(),
                    );

                    return;
                }

                if subgraph_instance.handle.is_finished() {
                    debug!(command_logger, "Subgraph is not running, starting");

                    *subgraph_instance = Self::start_subgraph(
                        logger,
                        cancel_token,
                        id,
                        deployment,
                        runner,
                        command_tx.clone(),
                    );

                    return;
                }

                debug!(command_logger, "Gracefully restarting subgraph");

                pending_start_commands.insert(
                    deployment.cheap_clone(),
                    Command::Start {
                        id,
                        deployment,
                        runner,
                    },
                );
            }
        }
    }

    /// Stops a subgraph.
    ///
    /// # Behaviour
    ///
    /// - If the subgraph is not active, does nothing
    /// - If the subgraph is active, sends a cancel signal to the active subgraph instance
    fn process_stop_command(
        logger_factory: &LoggerFactory,
        subgraph_instances: &mut HashMap<DeploymentLocator, SubgraphInstance>,
        pending_start_commands: &mut HashMap<DeploymentLocator, Command>,
        command: Command,
    ) {
        let Command::Stop { deployment } = command else {
            unreachable!();
        };

        let logger = logger_factory
            .subgraph_logger(&deployment)
            .new(slog::o!("command" => "stop"));

        if let Some(subgraph_instance) = subgraph_instances.get(&deployment) {
            debug!(logger, "Sending cancel signal");
            subgraph_instance.cancel_token.cancel();
        } else {
            debug!(logger, "Subgraph is not active");
        }

        pending_start_commands.remove(&deployment);
    }

    /// Removes a subgraph from the list of active subgraphs allowing the subgraph to be restarted.
    fn process_clear_command(
        logger_factory: &LoggerFactory,
        subgraph_instances: &mut HashMap<DeploymentLocator, SubgraphInstance>,
        pending_start_commands: &mut HashMap<DeploymentLocator, Command>,
        command_tx: &mpsc::UnboundedSender<Command>,
        command: Command,
    ) {
        let Command::Clear { id, deployment } = command else {
            unreachable!();
        };

        let logger = logger_factory
            .subgraph_logger(&deployment)
            .new(slog::o!("command" => "clear"));

        match subgraph_instances.get(&deployment) {
            Some(subgraph_instance) if subgraph_instance.id == id => {
                debug!(logger, "Removing active subgraph");
                subgraph_instances.remove(&deployment);
            }
            Some(_subgraph_instance) => {
                debug!(logger, "Active subgraph does not need to be removed");
                return;
            }
            None => {
                debug!(logger, "Subgraph is not active");
            }
        }

        if let Some(pending_start_command) = pending_start_commands.remove(&deployment) {
            debug!(logger, "Resending a pending start command");
            handle_send_result(&logger, command_tx.send(pending_start_command));
        }
    }

    /// Spawns a background task that executes the subgraph runner future.
    ///
    /// An additional background task is spawned to handle the graceful shutdown of the subgraph runner,
    /// and to ensure correct behaviour even if the subgraph runner panics.
    fn start_subgraph(
        logger: Logger,
        cancel_token: CancellationToken,
        id: u32,
        deployment: DeploymentLocator,
        runner: BoxRunner,
        command_tx: mpsc::UnboundedSender<Command>,
    ) -> SubgraphInstance {
        let mut runner_handle = tokio::spawn({
            let logger = logger.new(slog::o!("process" => "subgraph_runner"));
            let cancel_token = cancel_token.cheap_clone();

            async move {
                info!(logger, "Subgraph started");

                match runner(cancel_token).await {
                    Ok(()) => {
                        info!(logger, "Subgraph stopped");
                    }
                    Err(e) => {
                        error!(logger, "Subgraph failed";
                            "error" => ?e
                        );
                    }
                }
            }
        });

        let supervisor_handle = tokio::spawn({
            let logger = logger.new(slog::o!("process" => "subgraph_supervisor"));
            let cancel_token = cancel_token.cheap_clone();

            fn handle_runner_result(logger: &Logger, result: Result<(), tokio::task::JoinError>) {
                match result {
                    Ok(()) => {
                        debug!(logger, "Subgraph completed execution");
                    }
                    Err(e) if e.is_panic() => {
                        error!(logger, "Subgraph panicked";
                            "error" => ?e
                        );

                        // TODO: Maybe abort the entire process on panic and require a full graph-node restart.
                        //       Q: Should a bug that is triggered in a specific subgraph affect everything?
                        //       Q: How to make this failure loud enough so it is not missed?
                        //
                        // println!("Subgraph panicked");
                        // std::process::abort();
                    }
                    Err(e) => {
                        error!(logger, "Subgraph failed";
                            "error" => ?e
                        );
                    }
                }
            }

            async move {
                debug!(logger, "Subgraph supervisor started");

                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        debug!(logger, "Received cancel signal, waiting for subgraph to stop");

                        match timeout(SUBGRAPH_INSTANCE_GRACE_PERIOD, &mut runner_handle).await {
                            Ok(result) => {
                                handle_runner_result(&logger, result);
                            },
                            Err(_) => {
                                warn!(logger, "Subgraph did not stop after grace period, aborting");

                                runner_handle.abort();
                                let _ = runner_handle.await;

                                warn!(logger, "Subgraph aborted");
                            }
                        }
                    },
                    result = &mut runner_handle => {
                        handle_runner_result(&logger, result);
                        cancel_token.cancel();
                    }
                }

                debug!(logger, "Sending clear command");
                handle_send_result(&logger, command_tx.send(Command::Clear { id, deployment }));
            }
        });

        SubgraphInstance {
            id,
            handle: supervisor_handle,
            cancel_token,
        }
    }
}

impl Drop for Monitor {
    fn drop(&mut self) {
        // Send cancel signals to all active subgraphs so that they don't remain without an associated monitor
        self.cancel_token.cancel();
    }
}

/// Represents a background task that executes the subgraph runner future.
struct SubgraphInstance {
    id: u32,
    handle: JoinHandle<()>,
    cancel_token: CancellationToken,
}

/// Every command used by the subgraph monitor.
enum Command {
    /// A request to start executing the subgraph runner future.
    Start {
        id: u32,
        deployment: DeploymentLocator,
        runner: BoxRunner,
    },

    /// A request to stop executing the subgraph runner future.
    Stop { deployment: DeploymentLocator },

    /// A request to remove the subgraph from the list of active subgraphs.
    Clear {
        id: u32,
        deployment: DeploymentLocator,
    },
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Start {
                id,
                deployment,
                runner: _,
            } => f
                .debug_struct("Start")
                .field("id", id)
                .field("deployment", deployment)
                .finish_non_exhaustive(),
            Self::Stop { deployment } => f
                .debug_struct("Stop")
                .field("deployment", deployment)
                .finish(),
            Self::Clear { id, deployment } => f
                .debug_struct("Clear")
                .field("id", id)
                .field("deployment", deployment)
                .finish(),
        }
    }
}

fn handle_send_result(
    logger: &Logger,
    result: Result<(), tokio::sync::mpsc::error::SendError<Command>>,
) {
    match result {
        Ok(()) => {
            debug!(logger, "Command was sent successfully");
        }

        // This should only happen if the parent cancel token of the subgraph monitor was cancelled
        Err(e) => {
            error!(logger, "Failed to send command";
                "command" => ?e.0,
                "error" => ?e
            );
        }
    }
}
