use super::experiment::Experiment;
use crate::{
    AtomicBoolRelaxed, NodeStatus, blueprint::Blueprint, broadcast_router, de_u64, de_usize, env,
    zft,
};
use anyhow::{Context, Result};
use core::str;
use std::{
    collections::HashMap,
    fs::{self},
    path::{Path, PathBuf},
    sync::Arc,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tracing::{error, error_span, info, warn};

const BK_HB_IVL: Duration = Duration::from_secs(1);
const ND_TTL: Duration = Duration::from_secs(3);

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

pub struct Broker {
    ctx: zmq::Context,
    cl_router: zmq::Socket,
    nd_router: zmq::Socket,
    ex_router: zmq::Socket,

    nodes: HashMap<u64, Node>,
    experiments: HashMap<u64, ExpThread>,
    port_reg: Vec<Option<u64>>,
    zft_port_reg: Vec<Option<JoinHandle<Result<()>>>>,
    completed_exps: usize,
    last_maint: Instant,

    squid_exp_dir: PathBuf,
    once: bool,
}

impl Broker {
    pub fn new(once: bool) -> Result<Self> {
        let cpus = match thread::available_parallelism() {
            Ok(x) => x.get(),
            Err(_) => {
                warn!("Failed to find cpu count. Defaulting to 1");
                1
            }
        };

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

        let squid_exp_dir = get_squid_exp_path()?;
        fs::create_dir_all(&squid_exp_dir)?;

        Ok(Self {
            nodes: HashMap::new(),
            experiments: HashMap::new(),
            port_reg: std::iter::repeat_with(|| None).take(cpus).collect(),
            zft_port_reg: std::iter::repeat_with(|| None).take(10).collect(),
            completed_exps: 0,
            ctx,
            cl_router,
            nd_router,
            ex_router,
            last_maint: Instant::now(),
            squid_exp_dir,
            once,
        })
    }

    pub fn run(&mut self) {
        info!("🦑 Broker v{} starting up...", env!("CARGO_PKG_VERSION"));

        loop {
            match self.run_inner() {
                Ok(_) => break,
                Err(e) => error!("{:?}", e),
            }
        }
    }

    fn run_inner(&mut self) -> Result<()> {
        loop {
            if self.last_maint.elapsed() > BK_HB_IVL {
                self.maint()?;
                self.last_maint = Instant::now();

                if self.once && self.completed_exps > 0 {
                    broadcast_router(&self.nodes, &self.nd_router, &["kill".as_bytes()], 0)?;
                    break;
                }
            }

            if !self.pollin()? {
                continue;
            }

            self.process_client_q()?;
            self.process_node_q()?;
            self.process_experiment_q()?;
        }

        Ok(())
    }

    fn maint(&mut self) -> Result<()> {
        self.nodes.retain(|id, node| {
            if node.last_pulse.elapsed() > ND_TTL {
                warn!("⛓️‍💥 Lost node {:x}", id);
                false
            } else {
                let _ = self
                    .nd_router
                    .send_multipart([id.to_be_bytes().as_slice(), b"hb"], zmq::DONTWAIT);
                true
            }
        });

        self.experiments.retain(|_, exp| {
            if exp.handle.is_finished() {
                self.completed_exps += 1;
                self.port_reg[exp.port_reg_ix] = None;
                false
            } else {
                true
            }
        });

        for handle in &mut self.zft_port_reg {
            if handle.as_ref().is_some_and(|h| h.is_finished()) {
                let h = handle.take().unwrap();
                let res = h.join().unwrap();
                if let Err(e) = res {
                    error!("ZFT port thread error: {:?}", e);
                }
                *handle = None;
            }
        }

        Ok(())
    }

    fn pollin(&mut self) -> Result<bool> {
        let timeout = BK_HB_IVL
            .checked_sub(self.last_maint.elapsed())
            .map(|x| x.as_millis() as i64)
            .unwrap_or(0);

        let mut sockets = [
            self.cl_router.as_poll_item(zmq::POLLIN),
            self.nd_router.as_poll_item(zmq::POLLIN),
            self.ex_router.as_poll_item(zmq::POLLIN),
        ];

        Ok(zmq::poll(&mut sockets, timeout)? > 0)
    }

    fn process_client_q(&mut self) -> Result<()> {
        while let Ok(msgb) = self.cl_router.recv_multipart(zmq::DONTWAIT) {
            let clid_b = msgb[0].as_slice();
            let cmd = msgb[1].as_slice();
            match cmd {
                b"ping" => self.handle_client_ping(&msgb)?,
                b"run" => self.handle_client_run(&msgb)?,
                b"attach" => {
                    let exp_id = de_u64(&msgb[2])?;
                    let exp_id_x = format!("{:x}", exp_id);

                    info!("[CMD] attach {}", &exp_id_x);

                    let Some(exp_thread) = self.experiments.get(&exp_id) else {
                        self.cl_router.send_multipart(
                            [
                                clid_b,
                                b"error",
                                b"No running experiment with that ID. Cannot attach.",
                            ],
                            0,
                        )?;
                        return Ok(());
                    };

                    if exp_thread.client_connected.load() {
                        self.cl_router.send_multipart(
                            [
                                clid_b,
                                b"error",
                                b"A client is already connected to this experiment.",
                            ],
                            0,
                        )?;
                        return Ok(());
                    }

                    let port = 5600 + (exp_thread.port_reg_ix * 3);
                    let blueprint_b = rmp_serde::to_vec(&*exp_thread.blueprint)?;
                    self.cl_router.send_multipart(
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

                    let exp_dir = self.squid_exp_dir.join(exp_id_x);
                    if !exp_dir.exists() {
                        self.cl_router.send_multipart(
                            [
                                clid_b,
                                b"error",
                                b"No directory for that experiment ID exists.",
                            ],
                            0,
                        )?;
                        return Ok(());
                    }

                    let Some(ix) = self.zft_port_reg.iter().position(|e| e.is_none()) else {
                        self.cl_router.send_multipart(
                            [
                                clid_b,
                                b"error",
                                b"No available ZFT ports at this time. Try again later.",
                            ],
                            0,
                        )?;
                        return Ok(());
                    };
                    let port = (5700 + ix) as u32;

                    let ctx_clone = self.ctx.clone();
                    let sndthread = thread::spawn(move || -> Result<()> {
                        let ctx = ctx_clone;
                        let sndsock = ctx.socket(zmq::DEALER)?;
                        let zft_addr = format!("tcp://*:{}", port);
                        sndsock.bind(&zft_addr)?;
                        zft::send(exp_dir, &sndsock)?;
                        Ok(())
                    });
                    self.zft_port_reg[ix] = Some(sndthread);

                    self.cl_router
                        .send_multipart([clid_b, b"zft", &port.to_be_bytes()], 0)?;
                }
                _ => (),
            }
        }

        Ok(())
    }

    fn handle_client_ping(&mut self, msgb: &Vec<Vec<u8>>) -> Result<()> {
        info!("[CMD] ping");
        let clid_b = &msgb[0][..];
        if msgb.len() >= 4 {
            let major = str::from_utf8(&msgb[2])?;
            let minor = str::from_utf8(&msgb[3])?;
            if major == env!("CARGO_PKG_VERSION_MAJOR") && minor == env!("CARGO_PKG_VERSION_MINOR")
            {
                self.cl_router.send_multipart([clid_b, b"pong"], 0)?;
                return Ok(());
            }
        }

        self.cl_router
            .send_multipart([clid_b, b"pang", env!("CARGO_PKG_VERSION").as_bytes()], 0)?;

        Ok(())
    }

    fn handle_client_run(&mut self, msgb: &Vec<Vec<u8>>) -> Result<()> {
        let clid_b = &msgb[0][..];
        if clid_b.len() != 8 {
            self.cl_router.send_multipart(
                [
                    clid_b,
                    b"error",
                    b"Invalid router identity. Set socket identity to the u64 experiment ID.",
                ],
                0,
            )?;
            return Ok(());
        }

        let exp_id = de_u64(clid_b)?;
        let exp_id_b_own = exp_id.to_be_bytes();
        let exp_id_b = exp_id_b_own.as_slice();

        info!("[CMD] run {:x}", exp_id);

        if self.nodes.is_empty() {
            self.cl_router.send_multipart(
                [
                    clid_b,
                    b"error",
                    b"No available Squid nodes are connected at this time. Try again later.",
                ],
                0,
            )?;
            return Ok(());
        }

        let Some(ix) = self.port_reg.iter().position(|e| e.is_none()) else {
            self.cl_router.send_multipart(
                [
                    exp_id_b,
                    b"error",
                    b"No available experiment threads at this time. Try again later.",
                ],
                0,
            )?;
            return Ok(());
        };

        // first port is client, second is worker, third is supervisor
        let port = 5600 + ix * 3;
        self.port_reg[ix] = Some(exp_id);

        let blueprint_s = str::from_utf8(&msgb[3])?;
        let blueprint: Blueprint =
            toml::from_str(blueprint_s).context("Failed to deserialize blueprint toml string")?;
        let blueprint = Arc::new(blueprint);

        let client_connected = Arc::new(AtomicBoolRelaxed::new(true));

        let ctx_clone = self.ctx.clone();
        let exp_id_x_short = format!("{:x}", exp_id >> 32);
        let exp_span = error_span!("experiment", id = &exp_id_x_short);
        let squid_path = self.squid_exp_dir.clone();
        let blueprint_clone = Arc::clone(&blueprint);
        let client_connected_clone = Arc::clone(&client_connected);
        let once = self.once;
        let seeds: Vec<Vec<u8>> = serde_json::from_slice(&msgb[4])
            .context("Failed to deserialize seeds binary into Vec<Vec<u8>>")?;
        let handle = thread::spawn(move || {
            let _guard = exp_span.enter();
            let ctx = ctx_clone;
            let blueprint = blueprint_clone;

            let mut exp = Experiment::new(
                exp_id,
                blueprint,
                seeds,
                squid_path,
                ctx,
                port as u32,
                client_connected_clone,
                once,
            )?;

            exp.run()?;

            Ok(())
        });

        self.experiments.insert(
            exp_id,
            ExpThread {
                port_reg_ix: ix,
                handle,
                blueprint,
                client_connected,
            },
        );

        self.cl_router
            .send_multipart([exp_id_b, b"redirect", &(port as u32).to_be_bytes()], 0)?;

        info!("🧪 Starting experiment {:x}", exp_id);

        Ok(())
    }

    fn process_node_q(&mut self) -> Result<()> {
        while let Ok(msgb) = self.nd_router.recv_multipart(zmq::DONTWAIT) {
            let ndid_b = msgb[0].as_slice();
            let ndid = de_u64(ndid_b)?;
            let cmd = msgb[1].as_slice();
            match cmd {
                b"hb" => {
                    self.nodes
                        .entry(ndid)
                        .and_modify(|mg| mg.last_pulse = Instant::now());
                }
                b"register" => {
                    let num_cores = de_usize(&msgb[2])?;
                    self.nodes.insert(
                        ndid,
                        Node {
                            last_pulse: Instant::now(),
                            _status: NodeStatus::Idle,
                            _cores: num_cores,
                        },
                    );
                    self.nd_router.send_multipart([ndid_b, b"registered"], 0)?;
                    info!(
                        "🐋 Registered node {:x} with {} available cores",
                        ndid, num_cores
                    );
                }
                b"status" => {
                    let exp_id_b = msgb[2].as_slice();
                    let status_b = msgb[3].as_slice();

                    let status = NodeStatus::from(status_b[0]);
                    self.nodes.entry(ndid).and_modify(|nd| nd._status = status);

                    self.ex_router
                        .send_multipart([exp_id_b, b"ndstatus", ndid_b, status_b], 0)?;
                }
                _ => (),
            }
        }

        Ok(())
    }

    fn process_experiment_q(&mut self) -> Result<()> {
        while let Ok(msgb) = self.ex_router.recv_multipart(zmq::DONTWAIT) {
            let exp_id_b = msgb[0].as_slice();

            let cmd = msgb[1].as_slice();
            match cmd {
                b"ndspawn" => {
                    let task_image_b = msgb[2].as_slice();
                    let port_b = msgb[3].as_slice();
                    broadcast_router(
                        &self.nodes,
                        &self.nd_router,
                        &["spawn".as_bytes(), exp_id_b, task_image_b, port_b],
                        0,
                    )?;
                }
                b"ndabort" => {
                    broadcast_router(
                        &self.nodes,
                        &self.nd_router,
                        &["abort".as_bytes(), exp_id_b],
                        0,
                    )?;
                }
                _ => (),
            }
        }

        Ok(())
    }
}

pub fn get_squid_exp_path() -> Result<PathBuf> {
    let home_path = env("HOME")?;
    let squid_path = Path::new(&home_path).join(".local/share/squid/experiments");
    Ok(squid_path)
}
