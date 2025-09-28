mod client;
mod exp_manager;
mod tui;

use client::Client;

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;

pub struct ExpConfig {
    pub local: bool,
    pub test: bool,
    pub test_threads: usize,
    pub should_push: bool,
    pub show_tui: bool,
}

pub fn run(path: PathBuf, broker_addr: String, config: ExpConfig) -> Result<bool> {
    let client = Client::new(broker_addr);
    let experiment = client.prepare(path, &config)?;
    let experiment = Arc::new(experiment);
    let mut success = client.run(Arc::clone(&experiment), config.show_tui)?;

    if experiment.is_test {
        success = client.validate(&experiment)?;
    }

    Ok(success)
}

pub fn attach(broker_addr: String, id: String, out_dir: &Path, show_tui: bool) -> Result<bool> {
    let client = Client::new(broker_addr);
    let experiment = client.attach(&id, out_dir)?;
    let experiment = Arc::new(experiment);
    let success = client.run(Arc::clone(&experiment), show_tui)?;

    Ok(success)
}

pub fn fetch(broker_addr: String, id: String, out_dir: &Path) -> Result<()> {
    let client = Client::new(broker_addr);
    client.fetch(&id, out_dir)?;

    Ok(())
}
