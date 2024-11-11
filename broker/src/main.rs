mod blueprint;
mod ga;
mod util;

use anyhow::{bail, Result};
use blueprint::{Blueprint, WorkerResult};
use core::str;
use ga::{
    genome::{CTRNNGenome, CTRNNSpecies},
    Agent, GenericPopulation, Population,
};
use util::de_usize;

fn main() -> Result<()> {
    let ctx = zmq::Context::new();

    let fe_dealer = ctx.socket(zmq::DEALER)?;
    let be_pub = ctx.socket(zmq::PUB)?;
    let be_router = ctx.socket(zmq::ROUTER)?;

    fe_dealer.bind("tcp://*:5555")?;
    be_pub.bind("tcp://*:5556")?;
    be_router.bind("tcp://*:5557")?;

    loop {
        let res = run(&fe_dealer, &be_pub, &be_router);
        if let Err(e) = res {
            eprintln!("Error: {}", &e);
            fe_dealer.send_multipart(["error", &e.to_string()], 0)?;
        }
    }
}

fn run(fe_dealer: &zmq::Socket, be_pub: &zmq::Socket, be_router: &zmq::Socket) -> Result<()> {
    let msgb = fe_dealer.recv_multipart(0)?;
    let cmd = &msgb[0];
    if !cmd.starts_with("start".as_bytes()) {
        bail!("Received invalid command: `{}`", str::from_utf8(cmd)?);
    }

    println!("🧪 Starting experiment {}", str::from_utf8(&msgb[1])?);

    let blueprint: Blueprint = serde_json::from_slice(&msgb[2])?;
    let mut population = wake_population(&blueprint)?;
    let config = blueprint.ga_config;

    be_pub.send_multipart(&msgb[1..], 0)?;

    let mut sockets = [
        fe_dealer.as_poll_item(zmq::POLLIN),
        be_router.as_poll_item(zmq::POLLIN),
    ];
    let mut ready_workers = Vec::<Vec<u8>>::with_capacity(config.population_size);

    for i in 0..config.num_generations {
        println!("🧫 Running generation {}", i + 1);

        let mut queued_agent: usize = 0;
        let mut completed_sims: usize = 0;

        for worker in ready_workers.drain(..) {
            let cmd = "sim".as_bytes();
            let ix = queued_agent.to_le_bytes();
            let agent = population.pack_agent(queued_agent)?;
            be_router.send_multipart([worker.as_slice(), &[], cmd, &ix, &agent], 0)?;
            queued_agent += 1;
        }

        while completed_sims < config.population_size {
            zmq::poll(&mut sockets, -1)?;

            if sockets[1].is_readable() {
                let rmsgb = be_router.recv_multipart(0)?;
                let [src, dlm, msgb @ ..] = &rmsgb[..] else {
                    bail!("Router produced invalid message")
                };

                let cmd = str::from_utf8(&msgb[0])?;
                match cmd {
                    "ready" => {
                        if queued_agent < config.population_size {
                            let cmd = "sim".as_bytes();
                            let ix = queued_agent.to_le_bytes();
                            let agent = population.pack_agent(queued_agent)?;
                            be_router.send_multipart([src.as_slice(), dlm, cmd, &ix, &agent], 0)?;
                            queued_agent += 1;
                        } else {
                            ready_workers.push(src.clone())
                        }
                    }
                    "done" => {
                        let ix = de_usize(&msgb[1])?;
                        let result: WorkerResult = serde_json::from_slice(&msgb[2])?;
                        population.update_agent_score(ix, result.score);
                        completed_sims += 1;

                        // TODO: update client with progress (dontwait)
                    }
                    x => bail!("Received invalid command: {}", x),
                }
            }
        }

        population.evolve();
    }

    // TODO: aggregate results and send back to client

    for worker in ready_workers {
        let cmd = "kill".as_bytes();
        be_router.send_multipart([worker.as_slice(), &[], cmd], 0)?;
    }

    Ok(())
}

fn wake_population(blueprint: &Blueprint) -> Result<Box<dyn GenericPopulation>> {
    let population = match blueprint.genus.as_str() {
        "CTRNN" => synthesize!(CTRNNSpecies, CTRNNGenome, blueprint),
        x => bail!("Received unsupported genus: `{}`", x),
    };

    Ok(population)
}
