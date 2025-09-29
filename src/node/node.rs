use core::str;
use std::{
    collections::HashMap,
    sync::Arc,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use super::spawn_manager::SpawnManager;
use crate::{broadcast_router, de_u32, de_u64, docker};
use anyhow::Result;
use tracing::{debug, error, info, warn};

const BK_HB_INTERVAL: Duration = Duration::from_secs(1);
const BK_TTL: Duration = Duration::from_secs(5);

pub struct NodeMeta {
    #[allow(unused)]
    pub id: u64,
    pub num_threads: usize,
    pub local: bool,
    pub test: bool,
    pub broker_addr: String,
}

pub struct Node {
    meta: Arc<NodeMeta>,
    broker_url: String,
    done: bool,

    ctx: zmq::Context,
    bk_dealer: zmq::Socket,
    dr_dealer: zmq::Socket,

    dr_threads: HashMap<u64, JoinHandle<()>>,
    last_bk_hb_out: Instant,
    last_bk_hb_in: Instant,
}

impl Node {
    pub fn new(
        broker_addr: String,
        threads: Option<usize>,
        local: bool,
        test: bool,
    ) -> Result<Self> {
        let id: u64 = rand::random();
        let id_b = id.to_be_bytes();
        let id_x = format!("{:x}", id);

        let cores = thread::available_parallelism()?.get();
        let num_threads = threads
            .unwrap_or_else(|| if test { 1 } else { cores })
            .min(cores);
        let num_threads_s = num_threads.to_string();

        info!(
            "🐋 Squid node v{} starting with ID {}",
            env!("CARGO_PKG_VERSION"),
            &id_x
        );
        info!("🐋 Detected {} cores", cores);
        info!("🐋 Max workers: {}", &num_threads_s);

        let ctx = zmq::Context::new();

        let bk_dealer = ctx.socket(zmq::DEALER)?;
        let broker_url = format!("tcp://{}:5556", &broker_addr);
        bk_dealer.set_identity(&id_b)?;
        bk_dealer.connect(&broker_url)?;

        let dr_dealer = ctx.socket(zmq::ROUTER)?;
        dr_dealer.bind("inproc://docker")?;

        let meta = NodeMeta {
            id,
            num_threads,
            local,
            test,
            broker_addr,
        };

        let nd = Self {
            meta: Arc::new(meta),
            broker_url,
            done: false,
            ctx,
            bk_dealer,
            dr_dealer,
            dr_threads: HashMap::<u64, JoinHandle<()>>::new(),
            last_bk_hb_out: Instant::now(),
            last_bk_hb_in: Instant::now(),
        };

        Ok(nd)
    }

    pub fn run(&mut self) -> Result<()> {
        self.register()?;

        loop {
            if let Err(e) = self.run_inner() {
                error!("{:?}", e);
            }

            if self.done {
                break;
            }
        }

        Ok(())
    }

    pub fn run_inner(&mut self) -> Result<()> {
        loop {
            if self.last_bk_hb_out.elapsed() > BK_HB_INTERVAL {
                self.maint()?;
            };

            if !self.pollin()? {
                continue;
            }

            self.process_broker_q()?;
            if self.done {
                break;
            }
            self.process_docker_q()?;
        }

        Ok(())
    }

    fn register(&mut self) -> Result<()> {
        self.bk_dealer.send_multipart(
            [
                "register".as_bytes(),
                (self.meta.num_threads as u32).to_be_bytes().as_slice(),
            ],
            0,
        )?;

        let mut tries = 0;
        loop {
            let timeout = 5000 * (tries + 1).min(6);
            if self.bk_dealer.poll(zmq::POLLIN, timeout)? > 0 {
                let msgb = self.bk_dealer.recv_multipart(0)?;
                if msgb[0].starts_with(b"registered") {
                    info!("🦑 Registered with squid broker at {}", &self.broker_url);
                    break;
                } else {
                    warn!(
                        "Squid broker at {} sent invalid response to registration: `{}`",
                        &self.broker_url,
                        str::from_utf8(&msgb[0])?,
                    );
                }
            } else {
                warn!("Squid broker was unavailable at {}", &self.broker_url);
            };

            tries += 1;
            warn!("Retrying... (Attempt {})", tries);
        }

        Ok(())
    }

    fn maint(&mut self) -> Result<()> {
        if self.last_bk_hb_in.elapsed() > BK_TTL {
            warn!("⛓️‍💥 Lost connection to Squid broker. Reconnecting...");
            self.register()?;
        }
        self.bk_dealer.send("hb", 0)?;
        self.last_bk_hb_out = Instant::now();

        self.dr_threads.retain(|_, t| !t.is_finished());

        Ok(())
    }

    fn pollin(&mut self) -> Result<bool> {
        let mut sockets = [
            self.bk_dealer.as_poll_item(zmq::POLLIN),
            self.dr_dealer.as_poll_item(zmq::POLLIN),
        ];

        let timeout = BK_HB_INTERVAL
            .checked_sub(self.last_bk_hb_out.elapsed())
            .map(|x| x.as_millis() as i64)
            .unwrap_or(0);

        Ok(zmq::poll(&mut sockets, timeout)? > 0)
    }

    fn process_broker_q(&mut self) -> Result<()> {
        while let Ok(msgb) = self.bk_dealer.recv_multipart(zmq::DONTWAIT) {
            let cmd = msgb[0].as_slice();
            match cmd {
                b"hb" => {
                    self.last_bk_hb_in = Instant::now();
                }
                b"spawn" => {
                    let exp_id = de_u64(&msgb[1])?;
                    let exp_id_x = format!("{:x}", exp_id);
                    let task_image = String::from_utf8(msgb[2].clone())?;
                    let port = de_u32(&msgb[3])?; // first port of the group of 3

                    info!("Spawning container for experiment {}", &exp_id_x);
                    debug!("TASK IMAGE: {}", &task_image);
                    debug!("TASK PORT: {}", port);
                    debug!("TASK THREADS: {}", &self.meta.num_threads);
                    debug!("TASK BROKER ADDR: {}", &self.meta.broker_addr);

                    let meta = Arc::clone(&self.meta);
                    let ctx = self.ctx.clone();
                    let handle = thread::spawn(move || {
                        let mut sm =
                            SpawnManager::new(meta, ctx, exp_id, task_image, port).unwrap();
                        sm.run()
                    });

                    self.dr_threads.insert(exp_id, handle);
                }
                b"abort" => {
                    let exp_id_b = msgb[1].as_slice();
                    let exp_id = de_u64(exp_id_b)?;
                    let exp_id_hex = format!("{:x}", exp_id);

                    info!("Aborting experiment {}", &exp_id_hex);

                    self.dr_dealer.send_multipart([exp_id_b, b"abort"], 0)?;
                    self.dr_threads.remove(&exp_id);

                    let exp_id_label = format!("label=squid_exp_id={}", &exp_id_hex);
                    docker::kill_by_exp(&exp_id_label)?;
                }
                b"kill" => {
                    broadcast_router(&self.dr_threads, &self.dr_dealer, &["abort".as_bytes()], 0)?;
                    docker::kill_by_exp("label=squid_exp_id")?;
                    self.done = true;
                    return Ok(());
                }
                _ => (),
            }
        }

        Ok(())
    }

    fn process_docker_q(&mut self) -> Result<()> {
        while let Ok(msgb) = self.dr_dealer.recv_multipart(zmq::DONTWAIT) {
            let exp_id_b = msgb[0].as_slice();
            let cmd = msgb[1].as_slice();
            match cmd {
                b"status" => {
                    let status_b = msgb[2].as_slice();
                    self.bk_dealer
                        .send_multipart(["status".as_bytes(), exp_id_b, status_b], 0)?;
                }
                _ => (),
            }
        }

        Ok(())
    }
}
