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
use shared::{de_f64, de_u64, de_usize, Blueprint, ManagerStatus};
use std::{
    collections::HashMap,
    thread,
    time::{Duration, Instant},
};
use util::{de_router_id, to_router_id_array};

struct Manager {
    last_pulse: Instant,
    status: ManagerStatus,
    workers: usize,
}

/// Map of router identities to manager state
///
/// Keys are serialized u32s with a null byte at the start (byte array of length 5)
type Managers = HashMap<[u8; 5], Manager>;

fn main() -> Result<()> {
    println!("🦑 Broker starting up...");

    let mut managers = Managers::new();
    let mut cur_id: Option<u64> = None;

    let ctx = zmq::Context::new();

    let fe_router = ctx.socket(zmq::ROUTER)?;
    fe_router.bind("tcp://*:5555")?;

    let mg_router = ctx.socket(zmq::ROUTER)?;
    mg_router.bind("tcp://*:5556")?;

    let exp_sock = ctx.socket(zmq::PAIR)?;
    exp_sock.bind("inproc://experiment")?;

    let ctx_clone = ctx.clone();
    let _exp_thread = thread::spawn(move || -> Result<()> {
        let ctx = ctx_clone;
        let main_sock = ctx.socket(zmq::PAIR)?;
        main_sock.connect("inproc://experiment")?;

        let wk_router = ctx.socket(zmq::ROUTER)?;
        wk_router.bind("tcp://*:5557")?;

        loop {
            if let Err(e) = experiment_loop(&main_sock, &wk_router) {
                eprintln!("Error: {:?}", &e);
                main_sock.send_multipart(
                    [
                        "fe".as_bytes(),
                        "error".as_bytes(),
                        e.to_string().as_bytes(),
                    ],
                    0,
                )?;
            }
        }
    });

    let mut sockets = [
        fe_router.as_poll_item(zmq::POLLIN),
        mg_router.as_poll_item(zmq::POLLIN),
        exp_sock.as_poll_item(zmq::POLLIN),
    ];

    const HB_INTERVAL: Duration = Duration::from_secs(5);
    const MG_TTL: Duration = Duration::from_secs(15);
    let mut last_heartbeat = Instant::now();

    let mut main_loop = |cur_id: &mut Option<u64>| -> Result<()> {
        if last_heartbeat.elapsed() > HB_INTERVAL {
            managers.retain(|id, mg| {
                if mg.last_pulse.elapsed() > MG_TTL {
                    println!("⛓️‍💥 Lost manager {:x}", de_router_id(id).unwrap());
                    return false;
                };
                mg_router.send_multipart([&id[..], b"hb"], 0).unwrap();
                true
            });
            last_heartbeat = Instant::now();
        }

        let timeout = HB_INTERVAL
            .checked_sub(last_heartbeat.elapsed())
            .map(|x| x.as_millis() as i64)
            .unwrap_or(0)
            .max(1);

        zmq::poll(&mut sockets, timeout)?;

        if sockets[0].is_readable() {
            let msgb = fe_router.recv_multipart(0)?;
            let client = &msgb[0][..];
            let cmd = &msgb[1][..];
            match cmd {
                b"ping" => fe_router.send_multipart([client, b"pong"], 0)?,
                b"status" => {
                    let status: &[u8] = if cur_id.is_some() { b"busy" } else { b"idle" };
                    fe_router.send_multipart([client, b"status", status], 0)?;
                }
                b"run" => {
                    if client.len() != 8 {
                        fe_router.send_multipart([
                            client,
                            b"error",
                            b"Invalid router identity. Set socket identity to the u64 experiment ID."
                        ], 0)?;
                    }
                    let id = de_u64(client)?;
                    *cur_id = Some(id);
                    println!("🧪 Starting experiment {:x}", id);
                    exp_sock.send_multipart(&msgb[1..], 0)?;
                }
                b"abort" => {
                    if let Some(id) = cur_id {
                        println!("🧪 Aborting experiment {:x}", id);
                        let id_b = id.to_le_bytes();
                        // let abort_id = &msgb[1];
                        let abort_id = id.to_le_bytes();
                        // if abort_id == id {
                        broadcast_managers(
                            &managers,
                            &mg_router,
                            &["abort".as_bytes(), &abort_id],
                        )?;
                        exp_sock.send("abort", 0)?;
                        fe_router.send_multipart([&id_b[..], b"done", &abort_id], 0)?;
                        *cur_id = None;
                        // } else {
                        //     println!("Ignoring abort signal due to ID mismatch");
                        // }
                    } else {
                        println!("Ignoring abort signal, no experiments running");
                    }
                }
                _ => (),
            }
        }

        if sockets[1].is_readable() {
            let msgb = mg_router.recv_multipart(0)?;
            let mgid = to_router_id_array(msgb[0].clone())?;
            let cmd = msgb[1].as_slice();
            match cmd {
                b"hb" => {
                    managers
                        .entry(mgid)
                        .and_modify(|mg| mg.last_pulse = Instant::now());
                }
                b"register" => {
                    let num_workers = de_usize(&msgb[2])?;
                    managers.insert(
                        mgid,
                        Manager {
                            last_pulse: Instant::now(),
                            status: ManagerStatus::Idle,
                            workers: num_workers,
                        },
                    );
                    mg_router.send_multipart([&mgid[..], b"registered"], 0)?;
                    println!(
                        "🐋 Registered manager {:x} with {} workers",
                        de_router_id(&mgid)?,
                        num_workers
                    );
                }
                b"status" => {
                    let status = &msgb[2];
                    if let Some(id) = cur_id {
                        let id_b = id.to_le_bytes();
                        fe_router.send_multipart(
                            [&id_b[..], b"prog", &id_b, b"manager", &mgid[1..], status],
                            0,
                        )?;
                    }
                }
                _ => (),
            }
        }

        if sockets[2].is_readable() {
            let msgb = exp_sock.recv_multipart(0)?;
            let dst = msgb[0].as_slice();
            match dst {
                b"fe" => {
                    let cmd = msgb[1].as_slice();
                    let is_error = cmd == b"error";
                    let is_done = cmd == b"done";

                    if is_error {
                        let id = msgb[2].as_slice();
                        mg_router.send_multipart([b"abort", id], 0)?;
                        exp_sock.send("abort", 0)?;
                    }

                    if let Some(id) = cur_id {
                        fe_router.send(&id.to_le_bytes()[..], zmq::SNDMORE)?;
                        fe_router.send_multipart(msgb.into_iter().skip(1), 0)?;
                    }

                    if is_done || is_error {
                        *cur_id = None;
                    }
                }
                b"mg" => {
                    if managers.len() > 0 {
                        broadcast_managers(&managers, &mg_router, &msgb[1..])?;
                    } else {
                        // TODO UNDO THIS
                        // exp_sock.send("abort", 0)?;
                        // *cur_id = None;
                        // bail!("No managers connected, aborting experiment");
                    }
                }
                _ => (),
            }
        }

        Ok(())
    };

    loop {
        if let Err(e) = main_loop(&mut cur_id) {
            eprintln!("Error: {:?}", &e);
            if let Some(id) = cur_id {
                fe_router.send_multipart(
                    [&id.to_le_bytes()[..], b"error", e.to_string().as_bytes()],
                    0,
                )?;
            }
        }
    }
}

fn experiment_loop(main_sock: &zmq::Socket, wk_router: &zmq::Socket) -> Result<()> {
    let msgb = main_sock.recv_multipart(0)?;
    let cmd = &msgb[0];

    if cmd.starts_with(b"abort") {
        println!("Ignoring abort signal, no experiments running");
        return Ok(());
    }

    if !cmd.starts_with(b"run") {
        bail!("Received invalid command: `{}`", str::from_utf8(cmd)?);
    }

    let id = de_u64(&msgb[1])?;
    let id_b = id.to_le_bytes();

    // clear worker router in case of leftover messages (bandaid)
    while let Ok(_) = wk_router.recv_into(&mut [], zmq::DONTWAIT) {}

    let blueprint_s = str::from_utf8(&msgb[2])?;
    let blueprint: Blueprint =
        toml::from_str(blueprint_s).context("Failed to deserialize blueprint toml string")?;
    let seeds: Vec<Vec<u8>> = bincode::deserialize(&msgb[3])
        .context("Failed to deserialize seeds binary into Vec<Vec<u8>>")?;
    let mut population = wake_population(&blueprint, seeds)?;
    let config = blueprint.ga;

    main_sock.send_multipart(
        [
            "mg".as_bytes(),
            b"spawn",
            &id_b,
            blueprint.experiment.task_image.as_bytes(),
        ],
        0,
    )?;

    let mut sockets = [
        main_sock.as_poll_item(zmq::POLLIN),
        wk_router.as_poll_item(zmq::POLLIN),
    ];
    let mut ready_workers = Vec::<Vec<u8>>::with_capacity(config.population_size);

    for gen_num in 1..=config.num_generations {
        let gen_num_b = (gen_num as u32).to_le_bytes();
        main_sock.send_multipart(
            [
                "fe".as_bytes(),
                b"prog",
                &id_b,
                b"gen",
                &gen_num_b,
                b"running",
            ],
            0,
        )?;

        let mut queued_agent: usize = 0;
        let mut completed_sims: usize = 0;
        let mut best_fitness: f64 = 0.0;
        let mut best_agent_worker_id_b: Vec<u8> = vec![];
        let mut best_agent_ix: usize = 0;

        let min = usize::min(ready_workers.len(), config.population_size);
        for worker in ready_workers.drain(..min) {
            let ix = (queued_agent as u32).to_le_bytes();
            let agent = population.pack_agent(queued_agent)?;
            wk_router.send_multipart(
                [worker.as_slice(), &id_b, b"sim", &gen_num_b, &ix, &agent],
                0,
            )?;
            queued_agent += 1;

            main_sock.send_multipart(
                ["fe".as_bytes(), b"prog", &id_b, b"agent", &ix, b"running"],
                0,
            )?;
        }

        while completed_sims < config.population_size {
            zmq::poll(&mut sockets, -1)?;

            if sockets[0].is_readable() {
                let msgb = main_sock.recv_multipart(0)?;
                if msgb[0] == b"abort" {
                    return Ok(());
                }
            }

            if sockets[1].is_readable() {
                let msgb = wk_router.recv_multipart(0)?;

                if msgb[1] != id_b {
                    continue;
                }

                let cmd = msgb[2].as_slice();
                match cmd {
                    b"ready" => {
                        if queued_agent < config.population_size {
                            let ix = (queued_agent as u32).to_le_bytes();
                            let agent = population.pack_agent(queued_agent)?;
                            wk_router.send_multipart(
                                [msgb[0].as_slice(), &id_b, b"sim", &gen_num_b, &ix, &agent],
                                0,
                            )?;
                            queued_agent += 1;

                            main_sock.send_multipart(
                                ["fe".as_bytes(), b"prog", &id_b, b"agent", &ix, b"running"],
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
                            ["fe".as_bytes(), b"prog", &id_b, b"agent", &msgb[2], b"done"],
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
            wk_router.send_multipart(
                [
                    best_agent_worker_id_b.as_slice(),
                    &id_b,
                    b"moredata",
                    &gen_num_b,
                    &best_agent_ix.to_le_bytes(),
                ],
                0,
            )?;

            while wk_router.poll(zmq::POLLIN, 10000)? > 0 {
                let msgb = wk_router.recv_multipart(0)?;

                if msgb[1] != id_b {
                    continue;
                }

                let cmd = msgb[2].as_slice();
                match cmd {
                    b"moredata" => match msgb.into_iter().nth(5) {
                        Some(data) => {
                            main_sock.send_multipart(
                                ["fe".as_bytes(), b"save", &id_b, b"data", &gen_num_b, &data],
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
                "fe".as_bytes(),
                b"prog",
                &id_b,
                b"gen",
                &gen_num_b,
                b"done",
                bincode::serialize(&evaluation)?.as_slice(),
            ],
            0,
        )?;

        if gen_num % config.save_every == 0 {
            let agents = population.pack_save()?;
            main_sock
                .send_multipart(["fe".as_bytes(), b"save", &id_b, b"population", &agents], 0)?;
        }

        population.evolve();
    }

    let agents = population.pack_save()?;
    main_sock.send_multipart(["fe".as_bytes(), b"save", &id_b, b"population", &agents], 0)?;
    main_sock.send_multipart(["fe".as_bytes(), b"done", &id_b], 0)?;

    for worker in ready_workers {
        wk_router.send_multipart([&worker[..], &id_b, b"kill"], 0)?;
    }
    while let Ok(msgb) = wk_router.recv_multipart(zmq::DONTWAIT) {
        wk_router.send_multipart([&msgb[0][..], &id_b, b"kill"], 0)?;
    }

    println!("🧪 Finished experiment {:x}", id);

    Ok(())
}

fn broadcast_managers(
    managers: &Managers,
    mg_router: &zmq::Socket,
    msgb: &[impl Into<zmq::Message> + Clone],
) -> Result<()> {
    for id in managers.keys() {
        mg_router.send(&id[..], zmq::SNDMORE)?;
        mg_router.send_multipart(msgb.iter(), 0)?;
    }

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
