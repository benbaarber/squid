mod broker;
mod experiment;
mod ga;

use anyhow::Result;
use broker::Broker;

pub fn run(once: bool) -> Result<()> {
    let mut broker = Broker::new(once)?;
    broker.run();

    Ok(())
}
