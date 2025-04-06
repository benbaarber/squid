mod ga;
mod util;

use anyhow::{Context, Result, bail};
use core::str;
use ga::{
    agent::Agent,
    genome::{CTRNNGenome, CTRNNSpecies},
    population::{GenericPopulation, Population},
};
use serde::Deserialize;
use shared::{Blueprint, NodeStatus, de_f64, de_u64, de_usize};
use std::{
    collections::{HashMap, HashSet},
    ops::ControlFlow,
    thread,
    time::{Duration, Instant},
};

/// Squid node process
struct Node {
    last_pulse: Instant,
    status: NodeStatus,
    cores: usize,
}

/// Squid supervisor process
struct Supervisor {
    last_pulse: Instant,
    worker_ids: HashSet<u64>,
}

/// Squid worker process
struct Worker {
    supervisor_id: u64,
    agent_ix: Option<usize>,
}

fn main() -> Result<()> {
    println!("🦑 Broker starting up...");

    let mut nodes = HashMap::<u64, Node>::new();
    let mut cur_id: Option<u64> = None;

    let ctx = zmq::Context::new();

    let cl_router = ctx.socket(zmq::ROUTER)?;
    cl_router.bind("tcp://*:5555")?;

    let nd_router = ctx.socket(zmq::ROUTER)?;
    nd_router.bind("tcp://*:5556")?;

    let exp_sock = ctx.socket(zmq::PAIR)?;
    exp_sock.bind("inproc://experiment")?;

    let ctx_clone = ctx.clone();
    let _exp_thread = thread::spawn(move || -> Result<()> {
        let ctx = ctx_clone;
        let main_sock = ctx.socket(zmq::PAIR)?;
        main_sock.connect("inproc://experiment")?;

        loop {
            if let Err(e) = experiment_loop(&ctx, &main_sock) {
                eprintln!("Error: {:?}", &e);
                main_sock.send_multipart(
                    [
                        "cl".as_bytes(),
                        "error".as_bytes(),
                        e.to_string().as_bytes(),
                    ],
                    0,
                )?;
            }
        }
    });

    let mut sockets = [
        cl_router.as_poll_item(zmq::POLLIN),
        nd_router.as_poll_item(zmq::POLLIN),
        exp_sock.as_poll_item(zmq::POLLIN),
    ];

    const ND_HB_INTERVAL: Duration = Duration::from_secs(1);
    const ND_TTL: Duration = Duration::from_secs(3);
    let mut last_heartbeat = Instant::now();

    let mut main_loop = |cur_id: &mut Option<u64>| -> Result<()> {
        if last_heartbeat.elapsed() > ND_HB_INTERVAL {
            nodes.retain(|id, node| {
                if node.last_pulse.elapsed() > ND_TTL {
                    println!("⛓️‍💥 Lost node {:x}", id);
                    return false;
                };
                nd_router
                    .send_multipart([id.to_be_bytes().as_slice(), b"hb"], 0)
                    .unwrap();
                true
            });
            last_heartbeat = Instant::now();
        }

        let timeout = ND_HB_INTERVAL
            .checked_sub(last_heartbeat.elapsed())
            .map(|x| x.as_millis() as i64)
            .unwrap_or(0);

        zmq::poll(&mut sockets, timeout)?;

        if sockets[0].is_readable() {
            let msgb = cl_router.recv_multipart(0)?;
            let client = msgb[0].as_slice();
            let cmd = msgb[1].as_slice();
            match cmd {
                b"ping" => cl_router.send_multipart([client, b"pong"], 0)?,
                b"status" => {
                    let status: &[u8] = if cur_id.is_some() { b"busy" } else { b"idle" };
                    cl_router.send_multipart([client, b"status", status], 0)?;
                }
                b"run" => {
                    if client.len() != 8 {
                        cl_router.send_multipart([
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
                        let id_b = id.to_be_bytes();
                        // let abort_id = &msgb[1];
                        let abort_id_b = id.to_be_bytes();
                        // if abort_id == id {
                        broadcast_nodes(&nodes, &nd_router, &["abort".as_bytes(), &abort_id_b])?;
                        exp_sock.send("abort", 0)?;
                        cl_router.send_multipart([&id_b[..], b"done", &abort_id_b], 0)?;
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
            let msgb = nd_router.recv_multipart(0)?;
            let ndid_b = msgb[0].as_slice();
            let ndid = de_u64(ndid_b)?;
            let cmd = msgb[1].as_slice();
            match cmd {
                b"hb" => {
                    nodes
                        .entry(ndid)
                        .and_modify(|mg| mg.last_pulse = Instant::now());
                }
                b"register" => {
                    let num_cores = de_usize(&msgb[2])?;
                    nodes.insert(
                        ndid,
                        Node {
                            last_pulse: Instant::now(),
                            status: NodeStatus::Idle,
                            cores: num_cores,
                        },
                    );
                    nd_router.send_multipart([ndid_b, b"registered"], 0)?;
                    println!(
                        "🐋 Registered node {:x} with {} available cores",
                        ndid, num_cores
                    );
                }
                b"status" => {
                    let status = msgb[2].as_slice();
                    if let Some(id) = cur_id {
                        let id_b = id.to_be_bytes();
                        cl_router.send_multipart(
                            [id_b.as_slice(), b"prog", &id_b, b"node", ndid_b, status],
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
                b"cl" => {
                    let cmd = msgb[1].as_slice();
                    let is_error = cmd == b"error";
                    let is_done = cmd == b"done";

                    if is_error {
                        let id = msgb[2].as_slice();
                        nd_router.send_multipart([b"abort", id], 0)?;
                        exp_sock.send("abort", 0)?;
                    }

                    if let Some(id) = cur_id {
                        cl_router.send(id.to_be_bytes().as_slice(), zmq::SNDMORE)?;
                        cl_router.send_multipart(msgb.into_iter().skip(1), 0)?;
                    }

                    if is_done || is_error {
                        *cur_id = None;
                    }
                }
                b"nd" => {
                    if nodes.len() > 0 {
                        broadcast_nodes(&nodes, &nd_router, &msgb[1..])?;
                    } else {
                        // TODO UNCOMMENT THIS
                        // exp_sock.send("abort", 0)?;
                        // *cur_id = None;
                        // bail!("No nodes connected, aborting experiment");
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
                cl_router.send_multipart(
                    [
                        id.to_be_bytes().as_slice(),
                        b"error",
                        e.to_string().as_bytes(),
                    ],
                    0,
                )?;
            }
        }
    }
}

fn experiment_loop(ctx: &zmq::Context, main_sock: &zmq::Socket) -> Result<ControlFlow<()>> {
    let msgb = main_sock.recv_multipart(0)?;
    let cmd = &msgb[0];

    if cmd.starts_with(b"abort") {
        println!("Ignoring abort signal, no experiments running");
        return Ok(ControlFlow::Continue(()));
    }

    if !cmd.starts_with(b"run") {
        bail!("Received invalid command: `{}`", str::from_utf8(cmd)?);
    }

    let wk_router = ctx.socket(zmq::ROUTER)?;
    wk_router.bind("tcp://*:5600")?;

    let sv_router = ctx.socket(zmq::ROUTER)?;
    sv_router.bind("tcp://*:5601")?;

    let mut workers = HashMap::<u64, Worker>::new();
    let mut supervisors = HashMap::<u64, Supervisor>::new();

    let exp_id_b = msgb[1].as_slice();
    let exp_id = de_u64(exp_id_b)?;
    let exp_id_hex = format!("{:x}", exp_id);

    let blueprint_s = str::from_utf8(&msgb[2])?;
    let blueprint: Blueprint =
        toml::from_str(blueprint_s).context("Failed to deserialize blueprint toml string")?;
    let seeds: Vec<Vec<u8>> = serde_json::from_slice(&msgb[3])
        .context("Failed to deserialize seeds binary into Vec<Vec<u8>>")?;
    let mut population = wake_population(&blueprint, seeds)?;
    let config = blueprint.ga;

    main_sock.send_multipart(
        [
            "nd".as_bytes(),
            b"spawn",
            exp_id_b,
            blueprint.experiment.task_image.as_bytes(),
            b"5600",
        ],
        0,
    )?;

    const SV_HB_INTERVAL: Duration = Duration::from_secs(1);
    const SV_TTL: Duration = Duration::from_secs(3);
    let mut last_sv_hb_out = Instant::now();

    let mut sockets = [
        main_sock.as_poll_item(zmq::POLLIN),
        sv_router.as_poll_item(zmq::POLLIN),
        wk_router.as_poll_item(zmq::POLLIN),
    ];

    for gen_num in 1..=config.num_generations {
        let gen_num_b = (gen_num as u32).to_be_bytes();
        main_sock.send_multipart(
            [
                "cl".as_bytes(),
                b"prog",
                exp_id_b,
                b"gen",
                &gen_num_b,
                b"running",
            ],
            0,
        )?;

        let mut agent_queue = (0..config.population_size).rev().collect::<Vec<_>>();
        let mut completed_sims: usize = 0;
        let mut best_fitness: f64 = 0.0;
        let mut best_agent_wkid: u64 = 0;
        let mut best_agent_ix: usize = 0;

        let try_send_queued_agent = |wkid: u64,
                                     agent_queue: &mut Vec<usize>,
                                     population: &Box<dyn GenericPopulation>|
         -> Result<Option<usize>> {
            let opt_ix = agent_queue.pop();
            if let Some(next_ix) = opt_ix {
                let agent = population.pack_agent(next_ix)?;
                wk_router.send_multipart(
                    [
                        wkid.to_be_bytes().as_slice(),
                        b"sim",
                        &gen_num_b,
                        &(next_ix as u32).to_be_bytes(),
                        &agent,
                    ],
                    0,
                )?;
                main_sock.send_multipart(
                    [
                        "cl".as_bytes(),
                        b"prog",
                        exp_id_b,
                        b"agent",
                        &(next_ix as u32).to_be_bytes(),
                        b"running",
                    ],
                    0,
                )?;
            }
            Ok(opt_ix)
        };

        for (wkid, worker) in workers.iter_mut() {
            match try_send_queued_agent(*wkid, &mut agent_queue, &population)? {
                Some(ix) => worker.agent_ix = Some(ix),
                None => break,
            }
        }

        while completed_sims < config.population_size {
            if last_sv_hb_out.elapsed() > SV_HB_INTERVAL {
                supervisors.retain(|id, sv| {
                    let elapsed = sv.last_pulse.elapsed();
                    if elapsed < SV_HB_INTERVAL {
                        true
                    } else if elapsed > SV_TTL {
                        println!("⛓️‍💥 Lost supervisor {:x}", id);
                        false
                    } else {
                        sv_router
                            .send_multipart([id.to_be_bytes().as_slice(), b"hb"], 0)
                            .unwrap();
                        true
                    }
                });
                last_sv_hb_out = Instant::now();
            }

            let timeout = SV_HB_INTERVAL
                .checked_sub(last_sv_hb_out.elapsed())
                .map(|x| x.as_millis() as i64)
                .unwrap_or(0);

            if zmq::poll(&mut sockets, timeout)? == 0 {
                continue;
            };

            if sockets[0].is_readable() {
                let msgb = main_sock.recv_multipart(0)?;
                if msgb[0] == b"abort" {
                    // TODO make sure im not missing any cleanup here
                    return Ok(ControlFlow::Continue(()));
                }
            }

            if sockets[1].is_readable() {
                let msgb = sv_router.recv_multipart(0)?;
                let sv_id_b = msgb[0].as_slice();
                let sv_id = de_u64(sv_id_b)?;

                if msgb[1] != exp_id_b {
                    let bad_id = de_u64(&msgb[1])?;
                    println!(
                        "[Experiment {}] Supervisor {:x} sent mismatching experiment ID frame (sent `{:x}`). Killing supervisor.",
                        &exp_id_hex, sv_id, bad_id
                    );
                    sv_router.send_multipart([sv_id_b, b"kill"], 0)?;
                    supervisors.remove(&sv_id);
                    continue;
                }

                let cmd = msgb[2].as_slice();
                match cmd {
                    b"register" => {
                        let wk_ids: HashSet<u64> = serde_json::from_slice(&msgb[3])?;
                        let num_workers = wk_ids.len();

                        for id in &wk_ids {
                            workers.insert(
                                *id,
                                Worker {
                                    supervisor_id: sv_id,
                                    agent_ix: None,
                                },
                            );
                        }
                        supervisors.insert(
                            sv_id,
                            Supervisor {
                                last_pulse: Instant::now(),
                                worker_ids: wk_ids,
                            },
                        );
                        sv_router.send_multipart([sv_id_b, b"registered"], 0)?;
                        println!(
                            "[Experiment {}] Supervisor {:x} registered with {} workers",
                            &exp_id_hex, sv_id, num_workers,
                        );
                    }
                    b"dead" => {
                        let Some(sv) = supervisors.get_mut(&sv_id) else {
                            println!(
                                "[Experiment {}] Supervisor {:x} sent `dead` command but is not registered. Ignoring.",
                                &exp_id_hex, sv_id
                            );
                            continue;
                        };

                        let dead: Vec<u64> = serde_json::from_slice(&msgb[3])?;
                        let new: Vec<u64> = serde_json::from_slice(&msgb[4])?;

                        for wk_id in dead {
                            let worker = workers.remove(&wk_id);
                            sv.worker_ids.remove(&wk_id);
                            if let Some(worker) = worker {
                                if let Some(ix) = worker.agent_ix {
                                    agent_queue.push(ix);
                                }
                            }
                        }

                        for wk_id in new {
                            workers.insert(
                                wk_id,
                                Worker {
                                    supervisor_id: sv_id,
                                    agent_ix: None,
                                },
                            );
                            sv.worker_ids.insert(wk_id);
                        }
                    }
                    _ => (),
                }

                supervisors
                    .entry(sv_id)
                    .and_modify(|sv| sv.last_pulse = Instant::now());
            }

            if sockets[2].is_readable() {
                let msgb = wk_router.recv_multipart(0)?;
                let wk_id_b = msgb[0].as_slice();
                let wk_id = de_u64(wk_id_b)?;

                if !workers.contains_key(&wk_id) {
                    println!(
                        "[Experiment {}] Received message from unregistered worker {:x}. Killing worker.",
                        &exp_id_hex, wk_id
                    );
                    wk_router.send_multipart([wk_id_b, b"kill"], 0)?;
                    continue;
                }

                if msgb[1] != exp_id_b {
                    let bad_id = de_u64(&msgb[1])?;
                    println!(
                        "[Experiment {}] Worker {:x} sent mismatching experiment ID frame (sent `{:x}`). Killing worker.",
                        &exp_id_hex, wk_id, bad_id,
                    );
                    wk_router.send_multipart([wk_id_b, b"kill"], 0)?;
                    workers.remove(&wk_id);
                    continue;
                }

                let cmd = msgb[2].as_slice();
                match cmd {
                    b"init" => {
                        let opt_ix = try_send_queued_agent(wk_id, &mut agent_queue, &population)?;
                        workers.entry(wk_id).and_modify(|wk| wk.agent_ix = opt_ix);
                    }
                    b"sim" => {
                        let opt_ix = try_send_queued_agent(wk_id, &mut agent_queue, &population)?;
                        workers.entry(wk_id).and_modify(|wk| wk.agent_ix = opt_ix);

                        let ix = de_usize(&msgb[4])?;
                        let fitness = de_f64(&msgb[5])?;
                        population.update_agent_fitness(ix, fitness);
                        completed_sims += 1;
                        if fitness > best_fitness {
                            best_fitness = fitness;
                            best_agent_wkid = wk_id;
                            best_agent_ix = ix;
                        }

                        main_sock.send_multipart(
                            [
                                "cl".as_bytes(),
                                b"prog",
                                exp_id_b,
                                b"agent",
                                &msgb[4],
                                b"done",
                            ],
                            0,
                        )?;
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
                    best_agent_wkid.to_be_bytes().as_slice(),
                    b"data",
                    &gen_num_b,
                    (best_agent_ix as u32).to_be_bytes().as_slice(),
                ],
                0,
            )?;

            while wk_router.poll(zmq::POLLIN, 10000)? > 0 {
                let msgb = wk_router.recv_multipart(0)?;

                if msgb[1] != exp_id_b {
                    continue;
                }

                // TODO ensure wkid is best_agent_wkid

                let cmd = &msgb[2][..];
                match cmd {
                    b"data" => match msgb.into_iter().nth(5) {
                        Some(data) => {
                            main_sock.send_multipart(
                                [
                                    "cl".as_bytes(),
                                    b"save",
                                    exp_id_b,
                                    b"data",
                                    &gen_num_b,
                                    &data,
                                ],
                                0,
                            )?;
                            break;
                        }
                        None => bail!(
                            "Worker -> Broker protocol violated, 4th message frame for data missing"
                        ),
                    },
                    _ => (),
                }
            }
        }

        let evaluation = population.evaluate();
        main_sock.send_multipart(
            [
                "cl".as_bytes(),
                b"prog",
                exp_id_b,
                b"gen",
                &gen_num_b,
                b"done",
                &serde_json::to_vec(&evaluation)?,
            ],
            0,
        )?;

        if gen_num % config.save_every == 0 {
            let agents = population.pack_save()?;
            main_sock.send_multipart(
                ["cl".as_bytes(), b"save", exp_id_b, b"population", &agents],
                0,
            )?;
        }

        population.evolve();
    }

    let agents = population.pack_save()?;
    main_sock.send_multipart(
        ["cl".as_bytes(), b"save", exp_id_b, b"population", &agents],
        0,
    )?;
    main_sock.send_multipart(["cl".as_bytes(), b"done", exp_id_b], 0)?;

    for wk_id in workers.keys() {
        wk_router.send_multipart([wk_id.to_be_bytes().as_slice(), b"kill"], 0)?;
    }
    while let Ok(msgb) = wk_router.recv_multipart(zmq::DONTWAIT) {
        wk_router.send_multipart([msgb[0].as_slice(), b"kill"], 0)?;
    }
    for sv_id in supervisors.keys() {
        sv_router.send_multipart([sv_id.to_be_bytes().as_slice(), b"stop"], 0)?;
    }

    println!("🧪 Finished experiment {}", &exp_id_hex);

    Ok(ControlFlow::Continue(()))
}

fn broadcast_nodes(
    managers: &HashMap<u64, Node>,
    nd_router: &zmq::Socket,
    msgb: &[impl Into<zmq::Message> + Clone],
) -> Result<()> {
    for id in managers.keys() {
        nd_router.send(id.to_be_bytes().as_slice(), zmq::SNDMORE)?;
        nd_router.send_multipart(msgb.iter(), 0)?;
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
