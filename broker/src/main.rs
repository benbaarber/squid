mod ga;
mod util;

use anyhow::{bail, Context, Result};
use core::str;
use ga::{
    agent::Agent,
    genome::{CTRNNGenome, CTRNNSpecies},
    population::{GenericPopulation, Population},
};
use serde::Deserialize;
use shared::{de_f64, de_usize, Blueprint};
use std::thread;

fn main() -> Result<()> {
    println!("🦑 Broker starting up...");

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

        let mut id: Vec<u8> = vec![];

        loop {
            let res = experiment_loop(&main_sock, &be_pub, &be_router, &mut id);
            if let Err(e) = res {
                eprintln!("Error: {:?}", &e);
                main_sock.send_multipart(
                    ["error".as_bytes(), id.as_slice(), e.to_string().as_bytes()],
                    0,
                )?;
                be_pub.send_multipart([b"abort", id.as_slice()], 0)?;
                // main_sock.send_multipart([b"done", id.as_slice()], 0)?;
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
            x => bail!(
                "Received invalid command from client: {}",
                str::from_utf8(x)?
            ),
        }
    }

    if sockets[1].is_readable() {
        let msgb = exp_sock.recv_multipart(0)?;
        fe_dealer.send_multipart(msgb, 0)?;
    }

    Ok(())
}

fn experiment_loop(
    main_sock: &zmq::Socket,
    be_pub: &zmq::Socket,
    be_router: &zmq::Socket,
    id: &mut Vec<u8>,
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

    *id = msgb[1].clone();
    let id_str = str::from_utf8(id)?;
    println!("🧪 Starting experiment {}", id_str);

    // clear be router in case of leftover messages (bandaid)
    while let Ok(_) = be_router.recv_into(&mut [], zmq::DONTWAIT) {}

    let blueprint_s = str::from_utf8(&msgb[2])?;
    let blueprint: Blueprint =
        toml::from_str(blueprint_s).context("Failed to deserialize blueprint toml string")?;
    let seeds: Vec<Vec<u8>> = bincode::deserialize(&msgb[3])
        .context("Failed to deserialize seeds binary into Vec<Vec<u8>>")?;
    let mut population = wake_population(&blueprint, seeds)?;
    let config = blueprint.ga;

    be_pub.send_multipart(
        [
            b"spawn",
            id.as_slice(),
            blueprint.experiment.task_image.as_bytes(),
        ],
        0,
    )?;

    let mut sockets = [
        main_sock.as_poll_item(zmq::POLLIN),
        be_router.as_poll_item(zmq::POLLIN),
    ];
    let mut ready_workers = Vec::<Vec<u8>>::with_capacity(config.population_size);

    for gen_num in 1..=config.num_generations {
        let gen_num_b = (gen_num as u32).to_le_bytes();
        main_sock.send_multipart([b"prog", id.as_slice(), b"gen", &gen_num_b, b"running"], 0)?;

        let mut queued_agent: usize = 0;
        let mut completed_sims: usize = 0;
        let mut best_fitness: f64 = 0.0;
        let mut best_agent_worker_id_b: Vec<u8> = vec![];
        let mut best_agent_ix: usize = 0;

        let min = usize::min(ready_workers.len(), config.population_size);
        for worker in ready_workers.drain(..min) {
            let ix = (queued_agent as u32).to_le_bytes();
            let agent = population.pack_agent(queued_agent)?;
            be_router.send_multipart(
                [
                    worker.as_slice(),
                    id.as_slice(),
                    b"sim",
                    &gen_num_b,
                    &ix,
                    &agent,
                ],
                0,
            )?;
            queued_agent += 1;

            main_sock.send_multipart([b"prog", id.as_slice(), b"agent", &ix, b"running"], 0)?;
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

                if &msgb[1] != id {
                    continue;
                }

                let cmd = msgb[2].as_slice();
                match cmd {
                    b"ready" => {
                        if queued_agent < config.population_size {
                            let ix = (queued_agent as u32).to_le_bytes();
                            let agent = population.pack_agent(queued_agent)?;
                            be_router.send_multipart(
                                [
                                    msgb[0].as_slice(),
                                    &msgb[1],
                                    b"sim",
                                    &gen_num_b,
                                    &ix,
                                    &agent,
                                ],
                                0,
                            )?;
                            queued_agent += 1;

                            main_sock.send_multipart(
                                [b"prog", id.as_slice(), b"agent", &ix, b"running"],
                                0,
                            )?;
                        } else {
                            ready_workers
                                .push(msgb.into_iter().nth(0).expect("first element is router id"))
                        }
                    }
                    b"done" => {
                        let ix = de_usize(&msgb[4])?;
                        let fitness = de_f64(&msgb[5])?;
                        population.update_agent_fitness(ix, fitness);
                        completed_sims += 1;

                        main_sock.send_multipart(
                            [b"prog", id.as_slice(), b"agent", &msgb[2], b"done"],
                            0,
                        )?;

                        if fitness > best_fitness {
                            best_fitness = fitness;
                            best_agent_worker_id_b =
                                msgb.into_iter().nth(0).expect("first element is router id");
                            best_agent_ix = ix;
                        }
                    }
                    b"error" => {
                        // TODO: make resilient to crashes, right now one crash ends the whole experiment
                        let msg = str::from_utf8(&msgb[5])?;
                        bail!("{}", msg);
                    }
                    x => bail!("Received invalid command: {}", str::from_utf8(x)?),
                }
            }
        }

        if blueprint.csv_data.is_some() {
            be_router.send_multipart(
                [
                    best_agent_worker_id_b.as_slice(),
                    id.as_slice(),
                    b"moredata",
                    &gen_num_b,
                    &best_agent_ix.to_le_bytes(),
                ],
                0,
            )?;

            while be_router.poll(zmq::POLLIN, 10000)? > 0 {
                let msgb = be_router.recv_multipart(0)?;

                if &msgb[1] != id {
                    continue;
                }

                let cmd = msgb[2].as_slice();
                match cmd {
                    b"moredata" => match msgb.into_iter().nth(5) {
                        Some(data) => {
                            main_sock.send_multipart(
                                [b"save", id.as_slice(), b"data", &gen_num_b, &data],
                                0,
                            )?;
                            break;
                        }
                        None => bail!(
                        "Worker -> Broker protocol violated, 4th message frame for data missing"
                    ),
                    },
                    b"ready" => ready_workers
                        .push(msgb.into_iter().nth(0).expect("first element is router id")),
                    x => bail!(
                        "Received invalid command at end of generation: {}",
                        str::from_utf8(x)?
                    ),
                }
            }
        }

        let evaluation = population.evaluate();
        main_sock.send_multipart(
            [
                b"prog",
                id.as_slice(),
                b"gen",
                &gen_num_b,
                b"done",
                bincode::serialize(&evaluation)?.as_slice(),
            ],
            0,
        )?;

        if gen_num % config.save_every == 0 {
            let agents = population.pack_save()?;
            main_sock.send_multipart([b"save", id.as_slice(), b"population", &agents], 0)?;
        }

        population.evolve();
    }

    let agents = population.pack_save()?;
    main_sock.send_multipart([b"save", id.as_slice(), b"population", &agents], 0)?;
    main_sock.send_multipart([b"done", id.as_slice()], 0)?;

    for worker in ready_workers {
        be_router.send_multipart([worker.as_slice(), id.as_slice(), b"kill"], 0)?;
    }
    while let Ok(msgb) = be_router.recv_multipart(zmq::DONTWAIT) {
        be_router.send_multipart([msgb[0].as_slice(), id.as_slice(), b"kill"], 0)?;
    }

    println!("🧪 Finished experiment {}", id_str);

    Ok(())
}

fn wake_population(
    blueprint: &Blueprint,
    seeds: Vec<Vec<u8>>,
) -> Result<Box<dyn GenericPopulation>> {
    let population = match blueprint.experiment.genus.as_str() {
        "CTRNN" => synthesize!(CTRNNSpecies, CTRNNGenome, blueprint, seeds),
        x => bail!("Received unsupported genus: `{}`", x),
    };

    Ok(population)
}
