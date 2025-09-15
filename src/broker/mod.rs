mod ga;

use crate::{
    AtomicBoolRelaxed, ExpHistory, NodeStatus,
    blueprint::{Blueprint, NN},
    broadcast_router, create_exp_dir, de_f64, de_u32, de_u64, de_usize, env, zft,
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
    fs::{self, OpenOptions},
    ops::ControlFlow,
    path::{Path, PathBuf},
    sync::Arc,
    thread::{self, JoinHandle},
    time::{Duration, Instant, UNIX_EPOCH},
};
use tracing::{debug, error, error_span, info, trace, warn};

/// Squid node process
#[derive(Debug)]
struct Node {
    last_pulse: Instant,
    _status: NodeStatus,
    _cores: usize,
}

/// Squid experiment thread
#[derive(Debug)]
struct ExpThread {
    port_reg_ix: usize,
    handle: JoinHandle<Result<()>>,
    blueprint: Arc<Blueprint>,
    client_connected: Arc<AtomicBoolRelaxed>,
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

pub fn get_squid_exp_path() -> Result<PathBuf> {
    let home_path = env("HOME")?;
    let squid_path = Path::new(&home_path).join(".local/share/squid/experiments");
    Ok(squid_path)
}

pub fn run(once: bool) -> Result<()> {
    info!("🦑 Broker v{} starting up...", env!("CARGO_PKG_VERSION"));

    let squid_exp_dir = get_squid_exp_path()?;
    fs::create_dir_all(&squid_exp_dir)?;

    let mut nodes = HashMap::<u64, Node>::new();
    let mut experiments = HashMap::<u64, ExpThread>::new();
    let mut port_reg: Vec<Option<u64>> = std::iter::repeat_with(|| None)
        .take(thread::available_parallelism()?.get() - 1)
        .collect();
    let mut zft_port_reg: Vec<Option<JoinHandle<Result<()>>>> =
        std::iter::repeat_with(|| None).take(10).collect();
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

            for handle in &mut zft_port_reg {
                if handle.as_ref().is_some_and(|h| h.is_finished()) {
                    let h = handle.take().unwrap();
                    let res = h.join().unwrap();
                    if let Err(e) = res {
                        error!("ZFT port thread error: {:?}", e);
                    }
                    *handle = None;
                }
            }

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
            let clid_b = msgb[0].as_slice();
            let cmd = msgb[1].as_slice();
            match cmd {
                b"ping" => {
                    info!("[CMD] ping");
                    if msgb.len() >= 4 {
                        let major = str::from_utf8(&msgb[2])?;
                        let minor = str::from_utf8(&msgb[3])?;
                        if major == env!("CARGO_PKG_VERSION_MAJOR")
                            && minor == env!("CARGO_PKG_VERSION_MINOR")
                        {
                            cl_router.send_multipart([clid_b, b"pong"], 0)?;
                            return Ok(ControlFlow::Continue(()));
                        }
                    }

                    cl_router.send_multipart(
                        [clid_b, b"pang", env!("CARGO_PKG_VERSION").as_bytes()],
                        0,
                    )?;
                }
                b"run" => {
                    if clid_b.len() != 8 {
                        cl_router.send_multipart([
                            clid_b,
                            b"error",
                            b"Invalid router identity. Set socket identity to the u64 experiment ID."
                        ], 0)?;
                        return Ok(ControlFlow::Continue(()));
                    }

                    let exp_id = de_u64(clid_b)?;
                    let exp_id_b_own = exp_id.to_be_bytes();
                    let exp_id_b = exp_id_b_own.as_slice();

                    info!("[CMD] run {:x}", exp_id);

                    if nodes.is_empty() {
                        cl_router.send_multipart([
                            clid_b,
                            b"error",
                            b"No available Squid nodes are connected at this time. Try again later."
                        ], 0)?;
                        return Ok(ControlFlow::Continue(()));
                    }

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

                    let blueprint_s = str::from_utf8(&msgb[3])?;
                    let blueprint: Blueprint = toml::from_str(blueprint_s)
                        .context("Failed to deserialize blueprint toml string")?;
                    let blueprint = Arc::new(blueprint);

                    let client_connected = Arc::new(AtomicBoolRelaxed::new(true));

                    let ctx_clone = ctx.clone();
                    let exp_id_x = format!("{:x}", exp_id);
                    let exp_id_x_short = format!("{:x}", exp_id >> 32);
                    let exp_span = error_span!("experiment", id = &exp_id_x_short);
                    let squid_path = squid_exp_dir.clone();
                    let blueprint_clone = Arc::clone(&blueprint);
                    let client_connected_clone = Arc::clone(&client_connected);
                    let handle = thread::spawn(move || {
                        let _guard = exp_span.enter();
                        let ctx = ctx_clone;
                        let blueprint = blueprint_clone;
                        let cl_dealer = ctx.socket(zmq::DEALER)?;
                        let cl_addr = format!("tcp://*:{}", port);
                        cl_dealer.bind(&cl_addr)?;
                        info!("🔗 Client DEALER socket bound to {}", &cl_addr);

                        if let Err(e) = experiment(
                            exp_id,
                            exp_id_x,
                            &ctx,
                            &cl_dealer,
                            port,
                            &blueprint,
                            &msgb[4],
                            &squid_path,
                            once,
                            client_connected_clone,
                        ) {
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
                        ExpThread {
                            port_reg_ix: ix,
                            handle,
                            blueprint,
                            client_connected,
                        },
                    );

                    cl_router
                        .send_multipart([exp_id_b, b"redirect", &(port as u32).to_be_bytes()], 0)?;

                    info!("🧪 Starting experiment {:x}", exp_id);
                }
                b"attach" => {
                    let exp_id = de_u64(&msgb[2])?;
                    let exp_id_x = format!("{:x}", exp_id);

                    info!("[CMD] attach {}", &exp_id_x);

                    let Some(exp_thread) = experiments.get(&exp_id) else {
                        cl_router.send_multipart(
                            [
                                clid_b,
                                b"error",
                                b"No running experiment with that ID. Cannot attach.",
                            ],
                            0,
                        )?;
                        return Ok(ControlFlow::Continue(()));
                    };

                    if exp_thread.client_connected.load() {
                        cl_router.send_multipart(
                            [
                                clid_b,
                                b"error",
                                b"A client is already connected to this experiment.",
                            ],
                            0,
                        )?;
                        return Ok(ControlFlow::Continue(()));
                    }

                    let port = 5600 + (exp_thread.port_reg_ix * 3);
                    let blueprint_b = rmp_serde::to_vec(&*exp_thread.blueprint)?;
                    cl_router.send_multipart(
                        [
                            clid_b,
                            b"reattach",
                            &(port as u32).to_be_bytes(),
                            &blueprint_b,
                        ],
                        0,
                    )?;
                }
                b"fetch" => {
                    let exp_id = de_u64(&msgb[2])?;
                    let exp_id_x = format!("{:x}", exp_id);

                    info!("[CMD] fetch {}", exp_id);

                    let exp_dir = squid_exp_dir.join(exp_id_x);
                    if !exp_dir.exists() {
                        cl_router.send_multipart(
                            [
                                clid_b,
                                b"error",
                                b"No directory for that experiment ID exists.",
                            ],
                            0,
                        )?;
                        return Ok(ControlFlow::Continue(()));
                    }

                    let Some(ix) = zft_port_reg.iter().position(|e| e.is_none()) else {
                        cl_router.send_multipart(
                            [
                                clid_b,
                                b"error",
                                b"No available ZFT ports at this time. Try again later.",
                            ],
                            0,
                        )?;
                        return Ok(ControlFlow::Continue(()));
                    };
                    let port = (5700 + ix) as u32;

                    let ctx_clone = ctx.clone();
                    let sndthread = thread::spawn(move || -> Result<()> {
                        let ctx = ctx_clone;
                        let sndsock = ctx.socket(zmq::DEALER)?;
                        let zft_addr = format!("tcp://*:{}", port);
                        sndsock.bind(&zft_addr)?;
                        zft::send(exp_dir, &sndsock)?;
                        Ok(())
                    });
                    zft_port_reg[ix] = Some(sndthread);

                    cl_router.send_multipart([clid_b, b"zft", &port.to_be_bytes()], 0)?;
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
                    let status_b = msgb[3].as_slice();

                    let status = NodeStatus::from(status_b[0]);
                    nodes.entry(ndid).and_modify(|nd| nd._status = status);

                    ex_router.send_multipart([exp_id_b, b"ndstatus", ndid_b, status_b], 0)?;
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
    blueprint: &Blueprint,
    seeds_b: &[u8],
    squid_exp_path: &Path,
    once: bool,
    client_connected: Arc<AtomicBoolRelaxed>,
) -> Result<()> {
    let exp_id_b = exp_id.to_be_bytes();

    let mut history = ExpHistory {
        start: UNIX_EPOCH.elapsed()?.as_secs(),
        best_fitness_vec: Vec::with_capacity(blueprint.ga.num_generations),
        avg_fitness_vec: Vec::with_capacity(blueprint.ga.num_generations),
    };

    let bk_dealer = ctx.socket(zmq::DEALER)?;
    bk_dealer.set_identity(&exp_id_b)?;
    bk_dealer.connect("inproc://exp")?;

    // Wait for client redirect
    if cl_dealer.poll(zmq::POLLIN, 5000)? == 0 {
        bail!("Timed out waiting for client to redirect.");
    }

    let msgb = cl_dealer.recv_multipart(0)?;
    if msgb[0] == b"run" {
        let id = de_u64(&msgb[1])?;
        if id != exp_id {
            bail!("Client sent mismatching exp id {:x}", id);
        }
    } else {
        bail!(
            "Client sent wrong command after redirect, sent `{}`",
            str::from_utf8(&msgb[0])?
        );
    }

    // Deserialize seeds and population
    let seeds: Vec<Vec<u8>> = serde_json::from_slice(seeds_b)
        .context("Failed to deserialize seeds binary into Vec<Vec<u8>>")?;
    let mut population = wake_population(&blueprint, seeds)?;

    // Create outdir
    let out_dir = squid_exp_path.join(&exp_id_x);
    if !once {
        create_exp_dir(squid_exp_path, &exp_id_x, blueprint)?;
    }

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

    let config = &blueprint.ga;

    let save_population = |population: &Box<dyn GenericPopulation>| -> Result<()> {
        let agents = population.pack_save()?;

        if client_connected.load() {
            trace!("Sending population to client");
            cl_dealer.send_multipart(["save".as_bytes(), &exp_id_b, b"population", &agents], 0)?;
        }

        if !once {
            let agents_dir = out_dir.join("agents");
            // TODO dont serialize just to deserialize here.
            let agents: Vec<String> =
                serde_json::from_slice(&agents).context("failed to deserialize agents")?;
            for (i, agent) in agents.into_iter().enumerate() {
                let path = agents_dir.join(format!("agent_{}.json", i));
                fs::write(path, agent)?;
            }
        }
        Ok(())
    };

    let mut has_detached = false;

    let mut experiment_main_loop = || -> Result<ControlFlow<(), String>> {
        const HB_INTERVAL: Duration = Duration::from_secs(1);
        const SV_TTL: Duration = Duration::from_secs(5);
        const SV_GRACE: Duration = Duration::from_secs(5);
        const CL_TTL: Duration = Duration::from_secs(5);
        let mut last_hb = Instant::now();
        let mut last_cl_hb_in = Instant::now();
        let mut started = false; // prevent aborting before nodes pull docker images

        let mut sockets = [
            cl_dealer.as_poll_item(zmq::POLLIN),
            sv_router.as_poll_item(zmq::POLLIN),
            wk_router.as_poll_item(zmq::POLLIN),
            bk_dealer.as_poll_item(zmq::POLLIN),
        ];

        for gen_num in 1..=config.num_generations {
            let gen_num_b = (gen_num as u32).to_be_bytes();
            if client_connected.load() {
                cl_dealer.send_multipart(["prog".as_bytes(), b"gen", &gen_num_b, b"running"], 0)?;
            }

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
                    trace!("Sending agent {} to worker {:x}", next_ix, wkid);
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
                    // if !has_detached && client_connected.load() {
                    //     cl_dealer.send_multipart(
                    //         [
                    //             "prog".as_bytes(),
                    //             b"agent",
                    //             &(next_ix as u32).to_be_bytes(),
                    //             b"running",
                    //         ],
                    //         0,
                    //     )?;
                    // }
                }
                Ok(opt_ix)
            };

            for (wkid, worker) in workers.iter_mut() {
                match try_send_queued_agent(*wkid, &mut agent_stack, &population)? {
                    Some(ix) => worker.agent_ix = Some(ix),
                    None => break,
                }
            }

            let mut c = 0u32;
            while completed_sims < config.population_size {
                trace!("gen {} iteration {}", gen_num, {
                    let tmp = c;
                    c += 1;
                    tmp
                });
                if last_hb.elapsed() > HB_INTERVAL {
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
                        if elapsed < HB_INTERVAL {
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

                    if supervisors.len() == 0 && started {
                        bail!("All workers died. Aborting experiment.");
                    }

                    if client_connected.load() && last_cl_hb_in.elapsed() > CL_TTL {
                        warn!("Client disconnected");
                        has_detached = true;
                        client_connected.store(false);
                    }

                    last_hb = Instant::now();
                }

                let timeout = HB_INTERVAL
                    .checked_sub(last_hb.elapsed())
                    .map(|x| x.as_millis() as i64 + 1)
                    .unwrap_or(1);

                // prevents busy loop if sim time is very long
                if zmq::poll(&mut sockets, timeout)? == 0 {
                    continue;
                };

                while let Ok(msgb) = cl_dealer.recv_multipart(zmq::DONTWAIT) {
                    let cmd = msgb[0].as_slice();
                    match cmd {
                        b"hb" => {
                            last_cl_hb_in = Instant::now();
                        }
                        b"abort" => {
                            // TODO make sure im not missing any cleanup here
                            info!("Received abort signal from client. Aborting experiment.");
                            // bk_dealer.send_multipart(["ndabort".as_bytes()], 0)?;
                            broadcast_router(&supervisors, &sv_router, &["kill".as_bytes()])?;
                            return Ok(ControlFlow::Break(()));
                        }
                        b"attach" => {
                            info!("Client attached");
                            client_connected.store(true);
                            last_cl_hb_in = Instant::now();

                            cl_dealer.send_multipart(
                                ["prog".as_bytes(), b"history", &rmp_serde::to_vec(&history)?],
                                0,
                            )?;
                            cl_dealer.send_multipart(
                                [
                                    "prog".as_bytes(),
                                    b"short",
                                    &gen_num_b,
                                    &(completed_sims as u32).to_be_bytes(),
                                ],
                                0,
                            )?;
                            // todo send all evaluations so far
                        }
                        b"detach" => {
                            info!("Client detached");
                            has_detached = true;
                            client_connected.store(false);
                        }
                        _ => (),
                    }
                }

                while let Ok(msgb) = wk_router.recv_multipart(zmq::DONTWAIT) {
                    let wk_id_b = msgb[0].as_slice();
                    let wk_id = de_u64(wk_id_b)?;
                    let cmd = msgb[2].as_slice();

                    if cmd == b"init" {
                        if expected_workers.remove(&wk_id) {
                            workers.insert(wk_id, Worker { agent_ix: None });
                            trace!("Worker {:x} initialized", wk_id);
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

                            trace!("Worker {:x} finished simulation for agent {}", wk_id, ix);

                            if client_connected.load() {
                                cl_dealer.send_multipart(
                                    [
                                        "prog".as_bytes(),
                                        b"short",
                                        &gen_num_b,
                                        &(completed_sims as u32).to_be_bytes(),
                                    ],
                                    0,
                                )?;
                                // cl_dealer.send_multipart(
                                //     ["prog".as_bytes(), b"agent", &msgb[4], b"done"],
                                //     0,
                                // )?;
                            }
                        }
                        b"error" => {
                            if client_connected.load() {
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
                                        trace!("Pushing {} to retry stack", ix);
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

                            if !started && status_b[0] == NodeStatus::Active as u8 {
                                debug!("First container spawned");
                                started = true;
                                last_hb = Instant::now() + SV_GRACE;
                            }

                            if client_connected.load() {
                                cl_dealer.send_multipart(
                                    ["prog".as_bytes(), b"node", nd_id_b, status_b],
                                    0,
                                )?;
                            }
                        }
                        _ => (),
                    }
                }
            }

            // Get experimental data from best agent
            if blueprint.csv_data.is_some() {
                trace!("Requesting csv data for best agent");
                wk_router.send_multipart(
                    [
                        best_agent_wkid.to_be_bytes().as_slice(),
                        b"data",
                        &gen_num_b,
                        (best_agent_ix as u32).to_be_bytes().as_slice(),
                    ],
                    0,
                )?;

                while wk_router.poll(zmq::POLLIN, 5000)? > 0 {
                    let msgb = wk_router.recv_multipart(0)?;

                    if msgb[1] != exp_id_b {
                        continue;
                    }

                    // TODO ensure wkid is best_agent_wkid

                    let cmd = &msgb[2][..];
                    match cmd {
                        b"data" => match msgb.into_iter().nth(5) {
                            Some(data) => {
                                // Send data for save on client
                                if !has_detached && client_connected.load() {
                                    trace!("Sending csv data to client");
                                    cl_dealer.send_multipart(
                                        ["save".as_bytes(), &exp_id_b, b"data", &gen_num_b, &data],
                                        0,
                                    )?;
                                }

                                if !once {
                                    // Save data locally
                                    trace!("Saving csv data to disk");
                                    let data_dir = out_dir.join("data");
                                    let data: Option<HashMap<String, Vec<Vec<Option<f64>>>>> =
                                        serde_json::from_slice(&data).context(
                                            "serde_json failed to parse additional sim data",
                                        )?;
                                    if let Some(data) = data {
                                        for (name, rows) in data {
                                            let path = data_dir.join(&name).with_extension("csv");
                                            let file =
                                                OpenOptions::new().append(true).open(path)?;
                                            let mut wtr = csv::Writer::from_writer(file);
                                            for row in rows {
                                                wtr.write_field(gen_num.to_string())?;
                                                wtr.write_record(row.iter().map(|x| match x {
                                                    Some(n) => n.to_string(),
                                                    None => String::new(),
                                                }))?;
                                            }
                                            wtr.flush()?;
                                        }
                                    }
                                }

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

            trace!("Evaluating population");
            let evaluation = population.evaluate();

            history.best_fitness_vec.push(evaluation.best_fitness);
            history.avg_fitness_vec.push(evaluation.avg_fitness);

            // Save evaluation
            if !once {
                trace!("Saving evaluation to disk");
                let path = out_dir.join("data/fitness.csv");
                let file = OpenOptions::new().append(true).open(path)?;
                let mut wtr = csv::Writer::from_writer(file);
                wtr.write_record([
                    evaluation.avg_fitness.to_string(),
                    evaluation.best_fitness.to_string(),
                ])?;
                wtr.flush()?;
            }

            // Send evaluation
            if client_connected.load() {
                trace!("Sending evaluation to client");
                cl_dealer.send_multipart(
                    [
                        "prog".as_bytes(),
                        b"gen",
                        &gen_num_b,
                        b"done",
                        &serde_json::to_vec(&evaluation)?,
                    ],
                    0,
                )?;
            }

            if let Some(f) = config.fitness_threshold
                && evaluation.best_fitness >= f
            {
                info!("Population reached fitness threshold. Ending experiment.");
                return Ok(ControlFlow::Continue("threshold".to_string()));
            }

            if gen_num % config.save_every == 0 {
                trace!("Saving population");
                save_population(&population)?;
            }

            trace!("Evolving population");
            population.evolve()?;
        }

        Ok(ControlFlow::Continue("".to_string()))
    };

    let reason = match experiment_main_loop() {
        Ok(ControlFlow::Continue(reason)) => reason,
        Ok(ControlFlow::Break(_)) => return Ok(()),
        Err(e) => {
            bk_dealer.send("ndabort", 0)?;
            return Err(e);
        }
    };

    if has_detached {
        cl_dealer.send("zft", 0)?;
        zft::send(out_dir.clone(), cl_dealer)?;
    }

    save_population(&population)?;
    if client_connected.load() {
        cl_dealer.send_multipart(["done".as_bytes(), &exp_id_b, reason.as_bytes()], 0)?;
    }

    debug!("Killing workers and supervisors");
    broadcast_router(&workers, &wk_router, &["kill".as_bytes()])?;
    broadcast_router(&supervisors, &sv_router, &["stop".as_bytes()])?;

    info!("🧪 Finished experiment {}", &exp_id_x);

    Ok(())
}

macro_rules! synthesize {
    ($species_type:ty, $genome_type:ty, $blueprint:expr, $seeds:expr) => {{
        let species = <$species_type>::try_from($blueprint.nn)?;
        let agents = if $seeds.len() > 0 {
            let mut agents = Vec::with_capacity($seeds.len());
            for seed in $seeds {
                let genome: $genome_type =
                    serde_json::from_slice(&seed).context("Failed to deserialize seed json")?;
                let agent = Agent::new(genome);
                agents.push(agent);
            }
            Some(agents)
        } else {
            None
        };

        let population = Population::new($blueprint.ga.clone(), species, agents)?;
        Box::new(population)
    }};
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
