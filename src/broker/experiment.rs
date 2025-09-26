use super::ga::{
    agent::Agent,
    genome::{CTRNNGenome, CTRNNSpecies},
    population::{GenericPopulation, Population},
};
use crate::{
    AtomicBoolRelaxed, ExpHistory, NodeStatus, PopEvaluation,
    blueprint::{Blueprint, NN},
    broadcast_router, create_exp_dir, de_f64, de_u32, de_u64, save_exp_data_b, zft,
};
use anyhow::{Context, Result, bail};
use core::str;
use std::{
    collections::{HashMap, HashSet},
    fs::{self, OpenOptions},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, UNIX_EPOCH},
};
use tracing::{debug, error, info, trace, warn};

const EX_HB_IVL: Duration = Duration::from_secs(1);
const SV_TTL: Duration = Duration::from_secs(5);
const SV_GRACE: Duration = Duration::from_secs(5);
const CL_TTL: Duration = Duration::from_secs(5);

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

#[derive(Debug)]
struct Generation {
    num: u32,
    size: usize,
    agent_stack: Vec<usize>,
    completed_sims: u32,
    best_fitness: f64,
    best_agent_wkid: u64,
    best_agent_ix: u32,
}

impl Generation {
    fn new(size: usize) -> Self {
        Self {
            num: 0,
            size,
            agent_stack: (0..size).rev().collect(),
            completed_sims: 0,
            best_fitness: f64::NEG_INFINITY,
            best_agent_wkid: 0,
            best_agent_ix: 0,
        }
    }

    fn next(&mut self, num: u32) {
        *self = Self::new(self.size);
        self.num = num;
    }
}

pub struct Experiment {
    id: u64,
    id_b: [u8; 8],
    id_x: String,
    port: u32,
    blueprint: Arc<Blueprint>,
    out_dir: PathBuf,
    once: bool,
    abort: bool,

    population: Box<dyn GenericPopulation>,
    generation: Generation,
    history: ExpHistory,

    #[allow(unused)]
    ctx: zmq::Context,
    bk_dealer: zmq::Socket,
    cl_dealer: zmq::Socket,
    wk_router: zmq::Socket,
    sv_router: zmq::Socket,
    workers: HashMap<u64, Worker>,
    supervisors: HashMap<u64, Supervisor>,
    /// Worker ids that have been authorized by a supervisor but have not yet initialized
    expected_workers: HashSet<u64>,
    client_connected: Arc<AtomicBoolRelaxed>,
    has_started: bool,
    has_detached: bool,
    last_hb: Instant,
    last_cl_hb_in: Instant,
}

impl Experiment {
    pub fn new(
        id: u64,
        blueprint: Arc<Blueprint>,
        seeds: Vec<Vec<u8>>,
        squid_exp_path: PathBuf,
        ctx: zmq::Context,
        port: u32,
        client_connected: Arc<AtomicBoolRelaxed>,
        once: bool,
    ) -> Result<Self> {
        let id_b = id.to_be_bytes();
        let id_x = format!("{:x}", id);

        let bk_dealer = ctx.socket(zmq::DEALER)?;
        bk_dealer.set_identity(&id_b)?;
        bk_dealer.connect("inproc://exp")?;

        let cl_dealer = ctx.socket(zmq::DEALER)?;
        let cl_addr = format!("tcp://*:{}", port);
        cl_dealer.bind(&cl_addr)?;
        info!("🔗 Client DEALER socket bound to {}", &cl_addr);

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

        let population = wake_population(&blueprint, seeds)?;

        let out_dir = squid_exp_path.join(&id_x);
        if !once {
            create_exp_dir(&squid_exp_path, &id_x, &blueprint)?;
        }

        let generation = Generation::new(blueprint.ga.population_size);
        let history = ExpHistory {
            start: UNIX_EPOCH.elapsed()?.as_secs(),
            best_fitness_vec: Vec::with_capacity(blueprint.ga.num_generations),
            avg_fitness_vec: Vec::with_capacity(blueprint.ga.num_generations),
        };

        let exp = Self {
            id,
            id_b,
            id_x,
            port,
            blueprint,
            out_dir,
            once,
            abort: false,

            population,
            generation,
            history,

            ctx,
            cl_dealer,
            bk_dealer,
            wk_router,
            sv_router,
            workers: HashMap::new(),
            supervisors: HashMap::new(),
            expected_workers: HashSet::new(),
            has_started: false,
            has_detached: false,
            client_connected,
            last_hb: Instant::now(),
            last_cl_hb_in: Instant::now(),
        };

        Ok(exp)
    }

    pub fn run(&mut self) -> Result<()> {
        if let Err(e) = self.run_inner() {
            let err = e.to_string();
            error!("{:?}", &e);
            self.cl_dealer.send_multipart(
                ["error".as_bytes(), &[true as u8], err.as_bytes()],
                zmq::DONTWAIT,
            )?;
            broadcast_router(&self.supervisors, &self.sv_router, &["kill".as_bytes()], 0)?;
        }

        Ok(())
    }

    fn run_inner(&mut self) -> Result<()> {
        self.await_client()?;

        // Request containers through broker
        self.bk_dealer.send_multipart(
            [
                "ndspawn".as_bytes(),
                self.blueprint.experiment.image.as_bytes(),
                self.port.to_be_bytes().as_slice(),
            ],
            0,
        )?;

        let mut finish_reason = String::new();

        for g in 1..=self.blueprint.ga.num_generations {
            self.generation.next(g as u32);

            self.run_generation()?;

            if self.abort {
                return Ok(());
            }

            let evaluation = self.evaluate_and_save()?;

            if let Some(f) = self.blueprint.ga.fitness_threshold
                && evaluation.best_fitness >= f
            {
                info!("Population reached fitness threshold. Ending experiment.");
                finish_reason = "threshold".to_string();
                break;
            }

            self.population.evolve()?;
        }

        if self.has_detached {
            self.cl_dealer.send("zft", 0)?;
            zft::send(self.out_dir.clone(), &self.cl_dealer)?;
        }

        self.save_population()?;
        if self.client_connected.load() {
            self.cl_dealer
                .send_multipart(["done".as_bytes(), &self.id_b, finish_reason.as_bytes()], 0)?;
        }

        debug!("Killing workers and supervisors");
        broadcast_router(&self.workers, &self.wk_router, &["kill".as_bytes()], 0)?;
        broadcast_router(&self.supervisors, &self.sv_router, &["stop".as_bytes()], 0)?;

        info!("🧪 Finished experiment {}", &self.id_x);
        Ok(())
    }

    fn await_client(&mut self) -> Result<()> {
        // Wait for client redirect
        if self.cl_dealer.poll(zmq::POLLIN, 5000)? == 0 {
            bail!("Timed out waiting for client to redirect.");
        }

        let msgb = self.cl_dealer.recv_multipart(0)?;
        if msgb[0] == b"run" {
            let id = de_u64(&msgb[1])?;
            if id != self.id {
                bail!("Client sent mismatching exp id {:x}", id);
            }
        } else {
            bail!(
                "Client sent wrong command after redirect, sent `{}`",
                str::from_utf8(&msgb[0])?
            );
        }

        Ok(())
    }

    fn run_generation(&mut self) -> Result<()> {
        if self.client_connected.load() {
            self.cl_dealer.send_multipart(
                [
                    "prog".as_bytes(),
                    b"gen",
                    &self.generation.num.to_be_bytes(),
                    b"running",
                ],
                0,
            )?;
        }

        let wkids = self.workers.keys().copied().collect::<Vec<_>>();
        for wkid in wkids {
            match self.try_send_queued_agent(wkid)? {
                Some(ix) => self
                    .workers
                    .entry(wkid)
                    .and_modify(|wk| wk.agent_ix = Some(ix)),
                None => break,
            };
        }

        let mut c = 0u32;
        while self.generation.completed_sims < self.generation.size as u32 {
            trace!("gen {} iteration {}", self.generation.num, {
                let tmp = c;
                c += 1;
                tmp
            });

            if self.last_hb.elapsed() > EX_HB_IVL {
                self.maint()?;
                self.last_hb = Instant::now();
            }

            if !self.pollin()? {
                continue;
            }

            self.process_client_q()?;
            if self.abort {
                break;
            }
            self.process_worker_q()?;
            self.process_supervisor_q()?;
            self.process_broker_q()?;
        }

        Ok(())
    }

    fn maint(&mut self) -> Result<()> {
        self.supervisors.retain(|id, sv| {
            if sv.worker_ids.len() == 0 {
                warn!(
                    "⛓️‍💥 All workers of supervisor {:x} have died. Terminating supervisor.",
                    id
                );
                self.sv_router
                    .send_multipart([id.to_be_bytes().as_slice(), b"stop"], 0)
                    .unwrap();
                return false;
            }
            let elapsed = sv.last_pulse.elapsed();
            if elapsed < EX_HB_IVL {
                true
            } else if elapsed > SV_TTL {
                warn!("⛓️‍💥 Lost supervisor {:x}", id);
                false
            } else {
                self.sv_router
                    .send_multipart([id.to_be_bytes().as_slice(), b"hb"], 0)
                    .unwrap();
                true
            }
        });

        if self.supervisors.len() == 0 && self.has_started {
            bail!("All workers died. Aborting experiment.");
        }

        if self.client_connected.load() && self.last_cl_hb_in.elapsed() > CL_TTL {
            warn!("Client disconnected");
            self.has_detached = true;
            self.client_connected.store(false);
        }

        Ok(())
    }

    fn pollin(&mut self) -> Result<bool> {
        let mut sockets = [
            self.cl_dealer.as_poll_item(zmq::POLLIN),
            self.sv_router.as_poll_item(zmq::POLLIN),
            self.wk_router.as_poll_item(zmq::POLLIN),
            self.bk_dealer.as_poll_item(zmq::POLLIN),
        ];

        let timeout = EX_HB_IVL
            .checked_sub(self.last_hb.elapsed())
            .map(|x| x.as_millis() as i64 + 1)
            .unwrap_or(1);

        Ok(zmq::poll(&mut sockets, timeout)? > 0)
    }

    fn process_client_q(&mut self) -> Result<()> {
        while let Ok(msgb) = self.cl_dealer.recv_multipart(zmq::DONTWAIT) {
            let cmd = msgb[0].as_slice();
            match cmd {
                b"hb" => {
                    self.last_cl_hb_in = Instant::now();
                }
                b"abort" => {
                    info!("Received abort signal from client. Aborting experiment.");
                    broadcast_router(&self.supervisors, &self.sv_router, &["kill".as_bytes()], 0)?;
                    self.abort = true;
                    return Ok(());
                }
                b"attach" => {
                    info!("Client attached");
                    self.client_connected.store(true);
                    self.last_cl_hb_in = Instant::now();

                    self.cl_dealer.send_multipart(
                        [
                            "prog".as_bytes(),
                            b"history",
                            &rmp_serde::to_vec(&self.history)?,
                        ],
                        0,
                    )?;
                    self.cl_dealer.send_multipart(
                        [
                            "prog".as_bytes(),
                            b"short",
                            &(self.generation.num as u32).to_be_bytes(),
                            &(self.generation.completed_sims as u32).to_be_bytes(),
                        ],
                        0,
                    )?;
                }
                b"detach" => {
                    info!("Client detached");
                    self.has_detached = true;
                    self.client_connected.store(false);
                }
                _ => (),
            }
        }

        Ok(())
    }

    fn process_worker_q(&mut self) -> Result<()> {
        while let Ok(msgb) = self.wk_router.recv_multipart(zmq::DONTWAIT) {
            let wk_id_b = msgb[0].as_slice();
            let wk_id = de_u64(wk_id_b)?;
            let cmd = msgb[2].as_slice();

            if cmd == b"init" {
                if self.expected_workers.remove(&wk_id) {
                    self.workers.insert(wk_id, Worker { agent_ix: None });
                    trace!("Worker {:x} initialized", wk_id);
                } else {
                    warn!(
                        "Worker {:x} initialized but was not authorized by a supervisor. Killing worker.",
                        wk_id
                    );
                    if let Err(e) = self
                        .wk_router
                        .send_multipart([wk_id_b, b"kill"], zmq::DONTWAIT)
                    {
                        warn!("Failed to kill worker {:x}: {}", wk_id, e);
                    }
                }
                let opt_ix = self.try_send_queued_agent(wk_id)?;
                self.workers
                    .entry(wk_id)
                    .and_modify(|wk| wk.agent_ix = opt_ix);
                continue;
            }

            if !self.workers.contains_key(&wk_id) {
                warn!(
                    "Received message from unregistered worker {:x}. Killing worker.",
                    wk_id
                );
                self.wk_router.send_multipart([wk_id_b, b"kill"], 0)?;
                continue;
            }

            if msgb[1] != self.id_b {
                let bad_id = de_u64(&msgb[1])?;
                warn!(
                    "Worker {:x} sent mismatching experiment ID frame (sent `{:x}`). Killing worker.",
                    wk_id, bad_id,
                );
                self.wk_router.send_multipart([wk_id_b, b"kill"], 0)?;
                self.workers.remove(&wk_id);
                continue;
            }

            match cmd {
                b"sim" => {
                    let opt_ix = self.try_send_queued_agent(wk_id)?;
                    self.workers
                        .entry(wk_id)
                        .and_modify(|wk| wk.agent_ix = opt_ix);

                    let ix = de_u32(&msgb[4])?;
                    let fitness = de_f64(&msgb[5])?;
                    self.population.update_agent_fitness(ix as usize, fitness);
                    self.generation.completed_sims += 1;
                    if fitness > self.generation.best_fitness {
                        self.generation.best_fitness = fitness;
                        self.generation.best_agent_wkid = wk_id;
                        self.generation.best_agent_ix = ix;
                    }

                    trace!("Worker {:x} finished simulation for agent {}", wk_id, ix);

                    if self.client_connected.load() {
                        self.send_short_progress()?;
                        // cl_dealer.send_multipart(
                        //     ["prog".as_bytes(), b"agent", &msgb[4], b"done"],
                        //     0,
                        // )?;
                    }
                }
                b"error" => {
                    if self.client_connected.load() {
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
                        self.cl_dealer.send_multipart(
                            ["error".as_bytes(), &[false as u8], full_msg.as_bytes()],
                            0,
                        )?;
                    }
                }
                _ => (),
            }
        }

        Ok(())
    }

    fn process_supervisor_q(&mut self) -> Result<()> {
        while let Ok(msgb) = self.sv_router.recv_multipart(zmq::DONTWAIT) {
            let sv_id_b = msgb[0].as_slice();
            let sv_id = de_u64(sv_id_b)?;
            let cmd = msgb[2].as_slice();

            if msgb[1] != self.id_b {
                let bad_id = de_u64(&msgb[1])?;
                warn!(
                    "Supervisor {:x} sent mismatching experiment ID frame (sent `{:x}`). Killing supervisor.",
                    sv_id, bad_id
                );
                self.sv_router.send_multipart([sv_id_b, b"kill"], 0)?;
                self.supervisors.remove(&sv_id);
                continue;
            }

            if cmd == b"register" {
                let wk_ids: HashSet<u64> = serde_json::from_slice(&msgb[3])?;
                let num_workers = wk_ids.len();

                for id in &wk_ids {
                    self.expected_workers.insert(*id);
                }
                self.supervisors.insert(
                    sv_id,
                    Supervisor {
                        last_pulse: Instant::now().checked_add(SV_GRACE).unwrap(),
                        worker_ids: wk_ids,
                    },
                );
                self.sv_router
                    .send_multipart([sv_id_b, b"registered"], zmq::DONTWAIT)?;
                info!(
                    "Supervisor {:x} registered with {} workers",
                    sv_id, num_workers,
                );
                continue;
            }

            if !self.supervisors.contains_key(&sv_id) {
                warn!(
                    "Received message from unregistered supervisor {:x}. Killing supervisor.",
                    sv_id
                );
                self.sv_router.send_multipart([sv_id_b, b"kill"], 0)?;
                continue;
            }

            match cmd {
                b"dead" => {
                    // TODO notify client
                    let dead: Vec<u64> = serde_json::from_slice(&msgb[3])?;
                    for wk_id in dead {
                        warn!("Worker {:x} died", wk_id);
                        if let Some(worker) = self.workers.remove(&wk_id) {
                            if let Some(ix) = worker.agent_ix {
                                trace!("Pushing {} to retry stack", ix);
                                self.generation.agent_stack.push(ix);
                                // If there are any idle workers, retry agent immediately
                                if let Some((id, wk)) = self
                                    .workers
                                    .iter_mut()
                                    .find(|(_, wk)| wk.agent_ix.is_none())
                                {
                                    wk.agent_ix = Some(ix);
                                    let id = *id;
                                    self.try_send_queued_agent(id)?;
                                }
                            }
                        }
                        self.supervisors.entry(sv_id).and_modify(|sv| {
                            sv.worker_ids.remove(&wk_id);
                        });
                    }
                }
                _ => (),
            }

            self.supervisors
                .entry(sv_id)
                .and_modify(|sv| sv.last_pulse = Instant::now());
        }

        Ok(())
    }

    fn process_broker_q(&mut self) -> Result<()> {
        while let Ok(msgb) = self.bk_dealer.recv_multipart(zmq::DONTWAIT) {
            let cmd = msgb[0].as_slice();
            match cmd {
                b"ndstatus" => {
                    let nd_id_b = msgb[1].as_slice();
                    let status_b = msgb[2].as_slice();

                    if !self.has_started && status_b[0] == NodeStatus::Active as u8 {
                        debug!("First container spawned");
                        self.has_started = true;
                        self.last_hb = Instant::now() + SV_GRACE;
                    }

                    if self.client_connected.load() {
                        self.cl_dealer
                            .send_multipart(["prog".as_bytes(), b"node", nd_id_b, status_b], 0)?;
                    }
                }
                _ => (),
            }
        }

        Ok(())
    }

    fn send_short_progress(&self) -> Result<()> {
        self.cl_dealer.send_multipart(
            [
                "prog".as_bytes(),
                b"short",
                &(self.generation.num as u32).to_be_bytes(),
                &(self.generation.completed_sims as u32).to_be_bytes(),
            ],
            0,
        )?;

        Ok(())
    }

    fn try_send_queued_agent(&mut self, wkid: u64) -> Result<Option<usize>> {
        let opt_ix = self.generation.agent_stack.pop();
        if let Some(next_ix) = opt_ix {
            let agent = self.population.pack_agent(next_ix)?;
            trace!("Sending agent {} to worker {:x}", next_ix, wkid);
            self.wk_router.send_multipart(
                [
                    wkid.to_be_bytes().as_slice(),
                    b"sim",
                    &(self.generation.num as u32).to_be_bytes(),
                    &(next_ix as u32).to_be_bytes(),
                    &agent,
                ],
                0,
            )?;
        }
        Ok(opt_ix)
    }

    fn evaluate_and_save(&mut self) -> Result<PopEvaluation> {
        if self.blueprint.csv_data.is_some() {
            self.retrieve_exp_data()?;
        }

        trace!("Evaluating population");
        let evaluation = self.population.evaluate();

        self.history.best_fitness_vec.push(evaluation.best_fitness);
        self.history.avg_fitness_vec.push(evaluation.avg_fitness);

        if !self.once {
            trace!("Saving evaluation to disk");
            let path = self.out_dir.join("data/fitness.csv");
            let file = OpenOptions::new().append(true).open(path)?;
            let mut wtr = csv::Writer::from_writer(file);
            wtr.write_record([
                evaluation.avg_fitness.to_string(),
                evaluation.best_fitness.to_string(),
            ])?;
            wtr.flush()?;
        }

        if self.client_connected.load() {
            trace!("Sending evaluation to client");
            self.cl_dealer.send_multipart(
                [
                    "prog".as_bytes(),
                    b"gen",
                    &self.generation.num.to_be_bytes(),
                    b"done",
                    &serde_json::to_vec(&evaluation)?,
                ],
                0,
            )?;
        }

        if self.generation.num % self.blueprint.ga.save_every as u32 == 0 {
            trace!("Saving population");
            self.save_population()?;
        }

        Ok(evaluation)
    }

    fn save_population(&mut self) -> Result<()> {
        let agents = self.population.pack_save()?;

        if self.client_connected.load() {
            trace!("Sending population to client");
            self.cl_dealer
                .send_multipart(["save".as_bytes(), &self.id_b, b"population", &agents], 0)?;
        }

        if !self.once {
            let agents_dir = self.out_dir.join("agents");
            // TODO dont serialize just to deserialize here.
            let agents: Vec<String> =
                serde_json::from_slice(&agents).context("failed to deserialize agents")?;
            for (i, agent) in agents.into_iter().enumerate() {
                let path = agents_dir.join(format!("agent_{}.json", i));
                fs::write(path, agent)?;
            }
        }
        Ok(())
    }

    /// Retrieve the experimental data of the best agent from the worker it was simulated on and
    /// save it
    fn retrieve_exp_data(&mut self) -> Result<()> {
        trace!("Requesting csv data for best agent");
        self.wk_router.send_multipart(
            [
                self.generation.best_agent_wkid.to_be_bytes().as_slice(),
                b"data",
                &self.generation.num.to_be_bytes(),
                &self.generation.best_agent_ix.to_be_bytes(),
            ],
            0,
        )?;

        while self.wk_router.poll(zmq::POLLIN, 5000)? > 0 {
            let msgb = self.wk_router.recv_multipart(0)?;

            if msgb[1] != self.id_b {
                continue;
            }

            // TODO ensure wkid is best_agent_wkid

            let cmd = &msgb[2][..];
            if cmd != b"data" {
                warn!("Failed to retrieve exp data. Worker did not respond with `data` cmd.");
                break;
            }

            let data = &msgb[5][..];

            // Send data for save on client
            if !self.has_detached && self.client_connected.load() {
                trace!("Sending csv data to client");
                self.cl_dealer.send_multipart(
                    [
                        "save".as_bytes(),
                        &self.id_b,
                        b"data",
                        &self.generation.num.to_be_bytes(),
                        &data,
                    ],
                    0,
                )?;
            }

            if !self.once {
                // Save data locally
                trace!("Saving csv data to disk");
                let data_dir = self.out_dir.join("data");
                save_exp_data_b(data, &data_dir, self.generation.num)?;
            }

            break;
        }

        Ok(())
    }
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
