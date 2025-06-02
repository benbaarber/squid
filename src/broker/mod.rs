mod ga;
mod util;

use crate::{
    synthesize,
    util::{
        NodeStatus,
        blueprint::{Blueprint, NN},
        de_f64, de_u32, de_u64, de_usize,
    },
};
use anyhow::{Context, Result, bail};
use core::str;
use ga::{
    agent::Agent,
    genome::{CTRNNGenome, CTRNNSpecies},
    population::{GenericPopulation, Population},
};
use std::{
    collections::{HashMap, HashSet},
    ops::ControlFlow,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tracing::{debug, error, error_span, info, warn};

/// Squid node process
#[derive(Debug)]
struct Node {
    last_pulse: Instant,
    _status: NodeStatus,
    _cores: usize,
}

/// Squid experiment thread
#[derive(Debug)]
struct Experiment {
    port_reg_ix: usize,
    handle: JoinHandle<Result<()>>,
}

/// Squid supervisor process
#[derive(Debug)]
struct Supervisor {
    last_pulse: Instant,
    worker_ids: HashSet<u64>,
}

/// Squid worker process
#[derive(Debug)]
struct Worker {
    agent_ix: Option<usize>,
}

pub fn run(once: bool) -> Result<()> {
    info!("🦑 Broker starting up...");

    let mut nodes = HashMap::<u64, Node>::new();
    let mut experiments = HashMap::<u64, Experiment>::new();
    let mut port_reg: Vec<Option<u64>> = std::iter::repeat_with(|| None)
        .take(thread::available_parallelism()?.get() - 1)
        .collect();
    let mut completed_exps = 0;

    let ctx = zmq::Context::new();

    let cl_router = ctx.socket(zmq::ROUTER)?;
    let cl_addr = "tcp://*:5555";
    cl_router.bind(cl_addr)?;
    info!("🔗 Client ROUTER socket bound to {}", cl_addr);

    let nd_router = ctx.socket(zmq::ROUTER)?;
    let nd_addr = "tcp://*:5556";
    nd_router.bind(nd_addr)?;
    info!("🔗 Node ROUTER socket bound to {}", nd_addr);

    let ex_router = ctx.socket(zmq::ROUTER)?;
    ex_router.bind("inproc://exp")?;

    let mut sockets = [
        cl_router.as_poll_item(zmq::POLLIN),
        nd_router.as_poll_item(zmq::POLLIN),
        ex_router.as_poll_item(zmq::POLLIN),
    ];

    const HB_INTERVAL: Duration = Duration::from_secs(1);
    const ND_TTL: Duration = Duration::from_secs(3);
    let mut last_heartbeat = Instant::now();

    let mut main_loop = || -> Result<ControlFlow<()>> {
        if last_heartbeat.elapsed() > HB_INTERVAL {
            nodes.retain(|id, node| {
                if node.last_pulse.elapsed() > ND_TTL {
                    warn!("⛓️‍💥 Lost node {:x}", id);
                    false
                } else {
                    let _ = nd_router.send_multipart([id.to_be_bytes().as_slice(), b"hb"], 0);
                    true
                }
            });

            experiments.retain(|_, exp| {
                if exp.handle.is_finished() {
                    completed_exps += 1;
                    port_reg[exp.port_reg_ix] = None;
                    false
                } else {
                    true
                }
            });

            if once && completed_exps > 0 {
                broadcast_router(&nodes, &nd_router, &["kill".as_bytes()])?;
                return Ok(ControlFlow::Break(()));
            }

            last_heartbeat = Instant::now();
        }

        let timeout = HB_INTERVAL
            .checked_sub(last_heartbeat.elapsed())
            .map(|x| x.as_millis() as i64)
            .unwrap_or(0);

        zmq::poll(&mut sockets, timeout)?;

        if sockets[0].is_readable() {
            let msgb = cl_router.recv_multipart(0)?;
            let exp_id_b = msgb[0].as_slice();
            let cmd = msgb[1].as_slice();
            match cmd {
                b"ping" => cl_router.send_multipart([exp_id_b, b"pong"], 0)?,
                b"run" => {
                    if exp_id_b.len() != 8 {
                        cl_router.send_multipart([
                            exp_id_b,
                            b"error",
                            b"Invalid router identity. Set socket identity to the u64 experiment ID."
                        ], 0)?;
                    }

                    let exp_id = de_u64(exp_id_b)?;
                    let exp_id_b_own = exp_id.to_be_bytes();
                    let exp_id_b = exp_id_b_own.as_slice();

                    let Some(ix) = port_reg.iter().position(|e| e.is_none()) else {
                        cl_router.send_multipart(
                            [
                                exp_id_b,
                                b"error",
                                b"No available experiment threads at this time. Try again later.",
                            ],
                            0,
                        )?;
                        return Ok(ControlFlow::Continue(()));
                    };

                    // first port is client, second is worker, third is supervisor
                    let port = 5600 + ix * 3;
                    port_reg[ix] = Some(exp_id);

                    let ctx_clone = ctx.clone();
                    let exp_id_x = format!("{:x}", exp_id >> 32);
                    let exp_span = error_span!("experiment", id = &exp_id_x);
                    let handle = thread::spawn(move || {
                        let _guard = exp_span.enter();
                        let ctx = ctx_clone;
                        let cl_dealer = ctx.socket(zmq::DEALER)?;
                        let cl_addr = format!("tcp://*:{}", port);
                        cl_dealer.bind(&cl_addr)?;
                        info!("🔗 Client DEALER socket bound to {}", &cl_addr);

                        if let Err(e) =
                            experiment(exp_id, exp_id_x, &ctx, &cl_dealer, port, &msgb[3], &msgb[4])
                        {
                            let err = e.to_string();
                            error!("{}", &e);
                            cl_dealer.send_multipart(
                                ["error".as_bytes(), &[true as u8], err.as_bytes()],
                                zmq::DONTWAIT,
                            )?;
                        }

                        Ok(())
                    });

                    experiments.insert(
                        exp_id,
                        Experiment {
                            port_reg_ix: ix,
                            handle,
                        },
                    );

                    cl_router
                        .send_multipart([exp_id_b, b"redirect", &(port as u32).to_be_bytes()], 0)?;

                    info!("🧪 Starting experiment {:x}", exp_id);
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
                            _status: NodeStatus::Idle,
                            _cores: num_cores,
                        },
                    );
                    nd_router.send_multipart([ndid_b, b"registered"], 0)?;
                    info!(
                        "🐋 Registered node {:x} with {} available cores",
                        ndid, num_cores
                    );
                }
                b"status" => {
                    let exp_id_b = msgb[2].as_slice();
                    let status = msgb[3].as_slice();

                    ex_router.send_multipart([exp_id_b, b"ndstatus", ndid_b, status], 0)?;
                }
                _ => (),
            }
        }

        if sockets[2].is_readable() {
            let msgb = ex_router.recv_multipart(0)?;
            let exp_id_b = msgb[0].as_slice();

            let cmd = msgb[1].as_slice();
            match cmd {
                b"ndspawn" => {
                    let task_image_b = msgb[2].as_slice();
                    let port_b = msgb[3].as_slice();
                    broadcast_router(
                        &nodes,
                        &nd_router,
                        &["spawn".as_bytes(), exp_id_b, task_image_b, port_b],
                    )?;
                }
                b"ndabort" => {
                    broadcast_router(&nodes, &nd_router, &["abort".as_bytes(), exp_id_b])?;
                }
                _ => (),
            }
        }

        Ok(ControlFlow::Continue(()))
    };

    loop {
        match main_loop() {
            Ok(ControlFlow::Continue(_)) => continue,
            Ok(ControlFlow::Break(_)) => break,
            Err(e) => {
                error!("{:?}", &e);
            }
        }
    }

    Ok(())
}

fn experiment(
    exp_id: u64,
    exp_id_x: String,
    ctx: &zmq::Context,
    cl_dealer: &zmq::Socket,
    port: usize,
    blueprint_b: &[u8],
    seeds_b: &[u8],
) -> Result<()> {
    let exp_id_b = exp_id.to_be_bytes();

    let bk_dealer = ctx.socket(zmq::DEALER)?;
    bk_dealer.set_identity(&exp_id_b)?;
    bk_dealer.connect("inproc://exp")?;

    // Wait for client redirect
    if cl_dealer.poll(zmq::POLLIN, 5000)? > 0 {
        let msgb = cl_dealer.recv_multipart(0)?;
        let id = de_u64(&msgb[1])?;
        if msgb[0] == b"run" {
            if id != exp_id {
                bail!("Client sent mismatching exp id {:x}", &id);
            }
        } else {
            bail!(
                "Client sent wrong command after redirect, sent `{}`",
                str::from_utf8(&msgb[0])?
            );
        }
    } else {
        bail!("Timed out waiting for client to redirect.");
    }

    // Deserialize blueprint and population
    let blueprint_s = str::from_utf8(blueprint_b)?;
    let blueprint: Blueprint =
        toml::from_str(blueprint_s).context("Failed to deserialize blueprint toml string")?;
    let seeds: Vec<Vec<u8>> = serde_json::from_slice(seeds_b)
        .context("Failed to deserialize seeds binary into Vec<Vec<u8>>")?;
    let mut population = wake_population(&blueprint, seeds)?;

    // Request containers through broker
    bk_dealer.send_multipart(
        [
            "ndspawn".as_bytes(),
            blueprint.experiment.image.as_bytes(),
            (port as u32).to_be_bytes().as_slice(),
        ],
        0,
    )?;

    let wk_router = ctx.socket(zmq::ROUTER)?;
    wk_router.set_rcvhwm(1e6 as i32)?;
    wk_router.set_sndhwm(1e6 as i32)?;
    let wk_addr = format!("tcp://*:{}", port + 1);
    wk_router.bind(&wk_addr)?;
    info!("🔗 Worker ROUTER socket bound to {}", &wk_addr);

    let sv_router = ctx.socket(zmq::ROUTER)?;
    let sv_addr = format!("tcp://*:{}", port + 2);
    sv_router.bind(&sv_addr)?;
    info!("🔗 Supervisor ROUTER socket bound to {}", &sv_addr);

    let mut workers = HashMap::<u64, Worker>::new();
    let mut supervisors = HashMap::<u64, Supervisor>::new();

    // Worker ids that have been authorized by a supervisor but have not yet initialized
    let mut expected_workers = HashSet::<u64>::new();

    let config = blueprint.ga;

    let mut experiment_main_loop = || -> Result<ControlFlow<()>> {
        const SV_HB_INTERVAL: Duration = Duration::from_secs(1);
        const SV_TTL: Duration = Duration::from_secs(5);
        const SV_GRACE: Duration = Duration::from_secs(5);
        // TODO: not sure this is the best approach
        let mut last_sv_hb_out = Instant::now().checked_add(SV_GRACE).unwrap();

        let mut sockets = [
            cl_dealer.as_poll_item(zmq::POLLIN),
            sv_router.as_poll_item(zmq::POLLIN),
            wk_router.as_poll_item(zmq::POLLIN),
            bk_dealer.as_poll_item(zmq::POLLIN),
        ];

        for gen_num in 1..=config.num_generations {
            let gen_num_b = (gen_num as u32).to_be_bytes();
            cl_dealer.send_multipart(
                ["prog".as_bytes(), &exp_id_b, b"gen", &gen_num_b, b"running"],
                0,
            )?;

            let mut agent_stack = (0..config.population_size).rev().collect::<Vec<_>>();
            let mut completed_sims: usize = 0;
            let mut best_fitness: f64 = f64::NEG_INFINITY;
            let mut best_agent_wkid: u64 = 0;
            let mut best_agent_ix: usize = 0;

            let try_send_queued_agent = |wkid: u64,
                                         agent_stack: &mut Vec<usize>,
                                         population: &Box<dyn GenericPopulation>|
             -> Result<Option<usize>> {
                let opt_ix = agent_stack.pop();
                if let Some(next_ix) = opt_ix {
                    let agent = population.pack_agent(next_ix)?;
                    debug!("Sending agent {} to worker {:x}", next_ix, wkid);
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
                    cl_dealer.send_multipart(
                        [
                            "prog".as_bytes(),
                            &exp_id_b,
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
                match try_send_queued_agent(*wkid, &mut agent_stack, &population)? {
                    Some(ix) => worker.agent_ix = Some(ix),
                    None => break,
                }
            }

            let mut c = 0;
            while completed_sims < config.population_size {
                debug!("gen {} iteration {}", gen_num, c);
                c += 1;
                if last_sv_hb_out.elapsed() > SV_HB_INTERVAL {
                    supervisors.retain(|id, sv| {
                    if sv.worker_ids.len() == 0 {
                        warn!(
                            "⛓️‍💥 All workers of supervisor {:x} have died. Terminating supervisor.",
                            id
                        );
                        sv_router
                            .send_multipart([id.to_be_bytes().as_slice(), b"stop"], 0)
                            .unwrap();
                        return false;
                    }
                    let elapsed = sv.last_pulse.elapsed();
                    if elapsed < SV_HB_INTERVAL {
                        true
                    } else if elapsed > SV_TTL {
                        warn!("⛓️‍💥 Lost supervisor {:x}", id);
                        false
                    } else {
                        sv_router
                            .send_multipart([id.to_be_bytes().as_slice(), b"hb"], 0)
                            .unwrap();
                        true
                    }
                });
                    if supervisors.len() == 0 {
                        bail!("All workers died. Aborting experiment.");
                    }
                    last_sv_hb_out = Instant::now();
                }

                let timeout = SV_HB_INTERVAL
                    .checked_sub(last_sv_hb_out.elapsed())
                    .map(|x| x.as_millis() as i64 + 1)
                    .unwrap_or(1);

                // prevents busy loop if sim time is very long
                if zmq::poll(&mut sockets, timeout)? == 0 {
                    continue;
                };

                while let Ok(msgb) = cl_dealer.recv_multipart(zmq::DONTWAIT) {
                    if msgb[0] == b"abort" {
                        // TODO make sure im not missing any cleanup here
                        info!("Received abort signal from client. Aborting experiment.");
                        // bk_dealer.send_multipart(["ndabort".as_bytes()], 0)?;
                        broadcast_router(&supervisors, &sv_router, &["kill".as_bytes()])?;
                        return Ok(ControlFlow::Break(()));
                    }
                }

                while let Ok(msgb) = wk_router.recv_multipart(zmq::DONTWAIT) {
                    let wk_id_b = msgb[0].as_slice();
                    let wk_id = de_u64(wk_id_b)?;
                    let cmd = msgb[2].as_slice();

                    if cmd == b"init" {
                        if expected_workers.remove(&wk_id) {
                            workers.insert(wk_id, Worker { agent_ix: None });
                            info!("Worker {:x} initialized", wk_id);
                        } else {
                            warn!(
                                "Worker {:x} initialized but was not authorized by a supervisor. Killing worker.",
                                wk_id
                            );
                            if let Err(e) =
                                wk_router.send_multipart([wk_id_b, b"kill"], zmq::DONTWAIT)
                            {
                                warn!("Failed to kill worker {:x}: {}", wk_id, e);
                            }
                        }
                        let opt_ix = try_send_queued_agent(wk_id, &mut agent_stack, &population)?;
                        workers.entry(wk_id).and_modify(|wk| wk.agent_ix = opt_ix);
                        continue;
                    }

                    if !workers.contains_key(&wk_id) {
                        warn!(
                            "Received message from unregistered worker {:x}. Killing worker.",
                            wk_id
                        );
                        wk_router.send_multipart([wk_id_b, b"kill"], 0)?;
                        continue;
                    }

                    if msgb[1] != exp_id_b {
                        let bad_id = de_u64(&msgb[1])?;
                        warn!(
                            "Worker {:x} sent mismatching experiment ID frame (sent `{:x}`). Killing worker.",
                            wk_id, bad_id,
                        );
                        wk_router.send_multipart([wk_id_b, b"kill"], 0)?;
                        workers.remove(&wk_id);
                        continue;
                    }

                    match cmd {
                        b"sim" => {
                            let opt_ix =
                                try_send_queued_agent(wk_id, &mut agent_stack, &population)?;
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

                            debug!("Worker {:x} finished simulation for agent {}", wk_id, ix);

                            cl_dealer.send_multipart(
                                ["prog".as_bytes(), &exp_id_b, b"agent", &msgb[4], b"done"],
                                0,
                            )?;
                        }
                        b"error" => {
                            let cur_gen = de_u32(&msgb[3])?;
                            let agent_ix = de_u32(&msgb[4])?;
                            let msg = str::from_utf8(&msgb[5])?;
                            let full_msg = format!(
                                "Worker error ({:x})\nGen {} Agent {}\nMessage:\n{}",
                                wk_id,
                                cur_gen,
                                agent_ix + 1,
                                msg
                            );
                            cl_dealer.send_multipart(
                                ["error".as_bytes(), &[false as u8], full_msg.as_bytes()],
                                0,
                            )?;
                        }
                        _ => (),
                    }
                }

                while let Ok(msgb) = sv_router.recv_multipart(zmq::DONTWAIT) {
                    let sv_id_b = msgb[0].as_slice();
                    let sv_id = de_u64(sv_id_b)?;

                    if msgb[1] != exp_id_b {
                        let bad_id = de_u64(&msgb[1])?;
                        warn!(
                            "Supervisor {:x} sent mismatching experiment ID frame (sent `{:x}`). Killing supervisor.",
                            sv_id, bad_id
                        );
                        sv_router.send_multipart([sv_id_b, b"kill"], 0)?;
                        supervisors.remove(&sv_id);
                        continue;
                    }

                    if msgb[2] == b"register" {
                        let wk_ids: HashSet<u64> = serde_json::from_slice(&msgb[3])?;
                        let num_workers = wk_ids.len();

                        for id in &wk_ids {
                            expected_workers.insert(*id);
                        }
                        supervisors.insert(
                            sv_id,
                            Supervisor {
                                // TODO: dont hardcode supervisor duration
                                last_pulse: Instant::now().checked_add(SV_GRACE).unwrap(),
                                worker_ids: wk_ids,
                            },
                        );
                        sv_router.send_multipart([sv_id_b, b"registered"], 0)?;
                        info!(
                            "Supervisor {:x} registered with {} workers",
                            sv_id, num_workers,
                        );
                        continue;
                    }

                    if !supervisors.contains_key(&sv_id) {
                        warn!(
                            "Received message from unregistered supervisor {:x}. Killing supervisor.",
                            sv_id
                        );
                        sv_router.send_multipart([sv_id_b, b"kill"], 0)?;
                        continue;
                    }

                    let cmd = msgb[2].as_slice();
                    match cmd {
                        b"dead" => {
                            // TODO notify client
                            let dead: Vec<u64> = serde_json::from_slice(&msgb[3])?;
                            for wk_id in dead {
                                warn!("Worker {:x} died", wk_id);
                                if let Some(worker) = workers.remove(&wk_id) {
                                    if let Some(ix) = worker.agent_ix {
                                        debug!("Pushing {} to stack", ix);
                                        agent_stack.push(ix);
                                        // If there are any idle workers, retry agent immediately
                                        if let Some((id, wk)) =
                                            workers.iter_mut().find(|(_, wk)| wk.agent_ix.is_none())
                                        {
                                            try_send_queued_agent(
                                                *id,
                                                &mut agent_stack,
                                                &population,
                                            )?;
                                            wk.agent_ix = Some(ix);
                                        }
                                    }
                                }
                                supervisors.entry(sv_id).and_modify(|sv| {
                                    sv.worker_ids.remove(&wk_id);
                                });
                            }
                        }
                        _ => (),
                    }

                    supervisors
                        .entry(sv_id)
                        .and_modify(|sv| sv.last_pulse = Instant::now());
                }

                while let Ok(msgb) = bk_dealer.recv_multipart(zmq::DONTWAIT) {
                    let cmd = msgb[0].as_slice();
                    match cmd {
                        b"ndstatus" => {
                            let nd_id_b = msgb[1].as_slice();
                            let status_b = msgb[2].as_slice();
                            cl_dealer.send_multipart(
                                ["prog".as_bytes(), &exp_id_b, b"node", nd_id_b, status_b],
                                0,
                            )?;
                        }
                        _ => (),
                    }
                }
            }

            // Get experimental data from best agent
            if blueprint.csv_data.is_some() {
                debug!("Requesting csv data");
                wk_router.send_multipart(
                    [
                        best_agent_wkid.to_be_bytes().as_slice(),
                        b"data",
                        &gen_num_b,
                        (best_agent_ix as u32).to_be_bytes().as_slice(),
                    ],
                    0,
                )?;
                debug!("request sent");

                while wk_router.poll(zmq::POLLIN, 5000)? > 0 {
                    let msgb = wk_router.recv_multipart(0)?;
                    debug!("message received");

                    if msgb[1] != exp_id_b {
                        continue;
                    }

                    // TODO ensure wkid is best_agent_wkid

                    let cmd = &msgb[2][..];
                    match cmd {
                        b"data" => match msgb.into_iter().nth(5) {
                            Some(data) => {
                                cl_dealer.send_multipart(
                                    ["save".as_bytes(), &exp_id_b, b"data", &gen_num_b, &data],
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

            debug!("Received csv data");

            let evaluation = population.evaluate();
            cl_dealer.send_multipart(
                [
                    "prog".as_bytes(),
                    &exp_id_b,
                    b"gen",
                    &gen_num_b,
                    b"done",
                    &serde_json::to_vec(&evaluation)?,
                ],
                0,
            )?;

            if gen_num % config.save_every == 0 {
                let agents = population.pack_save()?;
                cl_dealer
                    .send_multipart(["save".as_bytes(), &exp_id_b, b"population", &agents], 0)?;
            }

            population.evolve()?;
        }

        Ok(ControlFlow::Continue(()))
    };

    match experiment_main_loop() {
        Ok(ControlFlow::Continue(_)) => (),
        Ok(ControlFlow::Break(_)) => return Ok(()),
        Err(e) => {
            bk_dealer.send("ndabort", 0)?;
            return Err(e);
        }
    }

    let agents = population.pack_save()?;
    cl_dealer.send_multipart(["save".as_bytes(), &exp_id_b, b"population", &agents], 0)?;
    cl_dealer.send_multipart(["done".as_bytes(), &exp_id_b], 0)?;

    info!("killing workers and supervisors");
    broadcast_router(&workers, &wk_router, &["kill".as_bytes()])?;
    broadcast_router(&supervisors, &sv_router, &["stop".as_bytes()])?;

    info!("🧪 Finished experiment {}", &exp_id_x);

    Ok(())
}

fn broadcast_router<T>(
    map: &HashMap<u64, T>,
    router: &zmq::Socket,
    msgb: &[impl Into<zmq::Message> + Clone],
) -> Result<()> {
    for id in map.keys() {
        router.send(id.to_be_bytes().as_slice(), zmq::SNDMORE)?;
        router.send_multipart(msgb, 0)?;
    }

    Ok(())
}

fn wake_population(
    blueprint: &Blueprint,
    seeds: Vec<Vec<u8>>,
) -> Result<Box<dyn GenericPopulation>> {
    let population = match blueprint.nn {
        NN::CTRNN { .. } => synthesize!(CTRNNSpecies, CTRNNGenome, blueprint, seeds),
    };

    Ok(population)
}
