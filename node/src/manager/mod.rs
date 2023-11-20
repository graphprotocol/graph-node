use graph::prelude::anyhow;

pub mod commands;

pub type CmdResult = Result<(), anyhow::Error>;
