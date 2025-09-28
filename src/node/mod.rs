mod node;
mod spawn_manager;

use node::Node;

use crate::docker;
use anyhow::Result;

pub fn run(broker_addr: String, threads: Option<usize>, local: bool, test: bool) -> Result<()> {
    assert!(docker::is_installed()?);

    let mut node = Node::new(broker_addr, threads, local, test)?;
    node.run();

    Ok(())
}
