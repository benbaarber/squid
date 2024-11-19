mod ga;
mod util;

use anyhow::{bail, Result};
use core::str;
use ga::{
    genome::{CTRNNGenome, CTRNNSpecies},
    Agent, GenericPopulation, Population,
};
use shared::{de_usize, Blueprint, Summary};
use std::thread;

fn main() -> Result<()> {
    let ctx = zmq::Context::new();

    let fe_dealer = ctx.socket(zmq::DEALER)?;
    fe_dealer.bind("tcp://*:5555")?;

    let exp_sock = ctx.socket(zmq::PAIR)?;
    exp_sock.bind("inproc://experiment")?;

    let _exp_thread = thread::spawn(move || -> Result<()> {
        let main_sock = ctx.socket(zmq::PAIR)?;
        main_sock.connect("inproc://experiment")?;

        let be_pub = ctx.socket(zmq::PUB)?;
        let be_router = ctx.socket(zmq::ROUTER)?;

        be_pub.bind("tcp://*:5556")?;
        be_router.bind("tcp://*:5557")?;

        loop {
            let res = experiment_loop(&main_sock, &be_pub, &be_router);
            if let Err(e) = res {
                eprintln!("Error: {}", &e);
                main_sock.send_multipart(
                    [
                        "relay".as_bytes(),
                        b"error",
                        b"TODO_fake_id",
                        e.to_string().as_bytes(),
                    ],
                    0,
                )?;
            }
        }
    });

    let mut sockets = [
        fe_dealer.as_poll_item(zmq::POLLIN),
        exp_sock.as_poll_item(zmq::POLLIN),
    ];

    loop {
        let res = client_loop(&mut sockets, &fe_dealer, &exp_sock);
        if let Err(e) = res {
            eprintln!("Error: {}", &e);
            fe_dealer.send_multipart([b"error", e.to_string().as_bytes()], 0)?;
        }
    }
}

fn client_loop(
    sockets: &mut [zmq::PollItem],
    fe_dealer: &zmq::Socket,
    exp_sock: &zmq::Socket,
) -> Result<()> {
    zmq::poll(sockets, -1)?;

    if sockets[0].is_readable() {
        let msgb = fe_dealer.recv_multipart(0)?;
        let cmd = msgb[0].as_slice();
        match cmd {
            b"ping" => {
                fe_dealer.send("pong", 0)?;
            }
            b"run" | b"abort" => {
                exp_sock.send_multipart(msgb, 0)?;
            }
            x => bail!("Received invalid command: {}", str::from_utf8(x)?),
        }
    }

    if sockets[1].is_readable() {
        let msgb = exp_sock.recv_multipart(0)?;
        let cmd = msgb[0].as_slice();
        match cmd {
            b"progress" | b"error" | b"done" => {
                fe_dealer.send_multipart(msgb, 0)?;
            }
            b"relay" => {
                fe_dealer.send_multipart(&msgb[1..], 0)?;
            }
            x => bail!("Received invalid command: {}", str::from_utf8(x)?),
        }
    }

    Ok(())
}

fn experiment_loop(
    main_sock: &zmq::Socket,
    be_pub: &zmq::Socket,
    be_router: &zmq::Socket,
) -> Result<()> {
    let msgb = main_sock.recv_multipart(0)?;
    let cmd = &msgb[0];

    if cmd.starts_with(b"abort") {
        println!("Ignoring abort signal, no experiments running");
        return Ok(());
    }

    if !cmd.starts_with(b"run") {
        bail!("Received invalid command: `{}`", str::from_utf8(cmd)?);
    }

    let id = &msgb[1];
    println!("🧪 Starting experiment {}", str::from_utf8(id)?);

    let blueprint: Blueprint = serde_json::from_slice(&msgb[2])?;
    let mut population = wake_population(&blueprint)?;
    let config = blueprint.ga_config;

    be_pub.send_multipart(
        [b"spawn", id.as_slice(), blueprint.task_image.as_bytes()],
        0,
    )?;

    let mut sockets = [
        main_sock.as_poll_item(zmq::POLLIN),
        be_router.as_poll_item(zmq::POLLIN),
    ];
    let mut ready_workers = Vec::<Vec<u8>>::with_capacity(config.population_size);

    for i in 0..config.num_generations {
        let gen_num_b = (i as u32 + 1).to_le_bytes();
        main_sock.send_multipart([b"progress", id.as_slice(), b"gen", &gen_num_b], 0)?;
        println!("🧫 Running generation {}", i + 1);

        let mut queued_agent: usize = 0;
        let mut completed_sims: usize = 0;

        let min = usize::min(ready_workers.len(), config.population_size);
        for worker in ready_workers.drain(..min) {
            let ix = (queued_agent as u32).to_le_bytes();
            let agent = population.pack_agent(queued_agent)?;
            be_router.send_multipart([worker.as_slice(), b"sim", &ix, &agent], 0)?;
            queued_agent += 1;

            main_sock.send_multipart([b"progress", id.as_slice(), b"agent", &ix, b"running"], 0)?;
        }

        while completed_sims < config.population_size {
            zmq::poll(&mut sockets, -1)?;

            if sockets[0].is_readable() {
                let msgb = main_sock.recv_multipart(0)?;
                let cmd = msgb[0].as_slice();
                match cmd {
                    b"abort" => {
                        // let abort_id = &msgb[1];
                        // if abort_id != id {
                        //     continue;
                        // }
                        let abort_id = id;

                        println!("🧪 Aborting experiment {}", str::from_utf8(abort_id)?);
                        be_pub.send_multipart([b"abort", abort_id.as_slice()], 0)?;
                        main_sock.send_multipart([b"done", abort_id.as_slice(), b"ABORTED"], 0)?;
                        return Ok(());
                    }
                    x => bail!("Received invalid command: {}", str::from_utf8(x)?),
                }
            }

            if sockets[1].is_readable() {
                let msgb = be_router.recv_multipart(0)?;

                let cmd = msgb[1].as_slice();
                match cmd {
                    b"ready" => {
                        if queued_agent < config.population_size {
                            let ix = (queued_agent as u32).to_le_bytes();
                            let agent = population.pack_agent(queued_agent)?;
                            be_router
                                .send_multipart([msgb[0].as_slice(), b"sim", &ix, &agent], 0)?;
                            queued_agent += 1;

                            main_sock.send_multipart(
                                [b"progress", id.as_slice(), b"agent", &ix, b"running"],
                                0,
                            )?;
                        } else {
                            ready_workers.push(msgb[0].clone())
                        }
                    }
                    b"done" => {
                        let ix = de_usize(&msgb[2])?;
                        let result: Summary = serde_json::from_slice(&msgb[3])?;
                        population.update_agent_score(ix, result.score);
                        completed_sims += 1;

                        main_sock.send_multipart(
                            [b"progress", id.as_slice(), b"agent", &msgb[2], b"done"],
                            0,
                        )?;
                    }
                    x => bail!("Received invalid command: {}", str::from_utf8(x)?),
                }
            }
        }

        population.evolve();
    }

    // TODO: aggregate results and send back to client
    let result = b"SUMMARY";
    main_sock.send_multipart([b"done", id.as_slice(), result], 0)?;

    for worker in ready_workers {
        be_router.send_multipart([worker.as_slice(), b"kill"], 0)?;
    }
    while let Ok(msgb) = be_router.recv_multipart(zmq::DONTWAIT) {
        be_router.send_multipart([msgb[0].as_slice(), b"kill"], 0)?;
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
