use std::sync::Arc;

use async_graphql::Context;
use async_graphql::Result;
use clap::Parser;
use graph::log::logger;
use graph::prelude::error;
use graph_store_postgres::connection_pool::ConnectionPool;
use graph_store_postgres::NotificationSender;
use graph_store_postgres::Store;
use graphman::config::Config;
use graphman::opt::Opt;

pub struct GraphmanContext {
    pub primary_pool: ConnectionPool,
    pub notification_sender: Arc<NotificationSender>,
    pub store: Arc<Store>,
    pub config: Config,
}

impl GraphmanContext {
    pub fn new(ctx: &Context<'_>) -> Result<GraphmanContext> {
        let primary_pool = ctx.data::<ConnectionPool>()?.to_owned();
        let notification_sender = ctx.data::<Arc<NotificationSender>>()?.to_owned();
        let store = ctx.data::<Arc<Store>>()?.to_owned();
        let opt = Opt::parse();
        let logger = logger(opt.debug);
        let config = match Config::load(&logger, &opt.clone().into()) {
            Ok(config) => config,
            Err(e) => {
                error!(logger, "Failed to load config due to: {}", e.to_string());
                return Err(e.into());
            }
        };

        Ok(GraphmanContext {
            primary_pool,
            notification_sender,
            store,
            config,
        })
    }
}
