use core::str;
use std::{
    fs::{self, OpenOptions},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{
    ExperimentMeta, NodeStatus, PopEvaluation, de_u8, de_u32, de_u64, save_exp_data_b, zft,
};
use anyhow::{Context, Result, bail};
use log::warn;
use tracing::{debug, error, info, trace};

const HB_INTERVAL: Duration = Duration::from_secs(1);

pub struct ExpManager {
    meta: Arc<ExperimentMeta>,
    ex_dealer: zmq::Socket,
    ui_dealer: zmq::Socket,
    last_hb: Instant,
    done: bool,
}

impl ExpManager {
    pub fn new(meta: Arc<ExperimentMeta>, ctx: zmq::Context) -> Result<Self> {
        let id_b = meta.id.to_be_bytes();
        let ui_dealer = ctx.socket(zmq::PAIR)?;
        ui_dealer.connect("inproc://experiment")?;

        let bk_dealer = ctx.socket(zmq::DEALER)?;
        bk_dealer.set_identity(&id_b)?;
        bk_dealer.connect(&meta.url)?;

        info!("🔗 Experiment DEALER socket connected to {}", &meta.url);

        let em = Self {
            meta,
            ex_dealer: bk_dealer,
            ui_dealer,
            last_hb: Instant::now(),
            done: false,
        };

        Ok(em)
    }

    pub fn run(&mut self) -> bool {
        match self.run_inner() {
            Ok(s) => s,
            Err(e) => {
                error!("Exp loop error: {}", e.to_string());
                self.ui_dealer.send("crashed", 0).unwrap();
                false
            }
        }
    }

    fn run_inner(&mut self) -> Result<bool> {
        if self.meta.is_new {
            self.ex_dealer
                .send_multipart(["run".as_bytes(), &self.meta.id.to_be_bytes()], 0)?;
        } else {
            self.ex_dealer.send("attach", 0)?;
        }

        loop {
            if self.last_hb.elapsed() >= HB_INTERVAL {
                self.ex_dealer.send("hb", 0)?;
                self.last_hb = Instant::now();
            }

            if !self.pollin()? {
                continue;
            }

            self.process_experiment_q()?;
            self.process_ui_q()?;

            if self.done {
                break;
            }
        }

        Ok(true)
    }

    fn pollin(&mut self) -> Result<bool> {
        let mut sockets = [
            self.ex_dealer.as_poll_item(zmq::POLLIN),
            self.ui_dealer.as_poll_item(zmq::POLLIN),
        ];

        let timeout = HB_INTERVAL
            .checked_sub(self.last_hb.elapsed())
            .map(|x| x.as_millis() as i64 + 1)
            .unwrap_or(1);

        Ok(zmq::poll(&mut sockets, timeout)? > 0)
    }

    fn process_experiment_q(&mut self) -> Result<()> {
        let mut i = 0;
        while let Ok(msgb) = self.ex_dealer.recv_multipart(zmq::DONTWAIT) {
            let cmd = msgb[0].as_slice();
            match cmd {
                b"prog" => {
                    trace!("[CLI EXP] prog");
                    self.ui_dealer.send_multipart(&msgb[1..], 0)?;
                    handle_prog(&msgb, &self.meta.out_dir)?;
                }
                b"save" => {
                    trace!("[CLI EXP] save");
                    let save_type = msgb[2].as_slice();
                    match save_type {
                        b"population" => handle_save_population(&msgb, &self.meta.out_dir)?,
                        b"data" => handle_save_data(&msgb, &self.meta.out_dir)?,
                        _ => (),
                    }
                }
                b"zft" => {
                    trace!("[CLI EXP] zft");
                    zft::recv(self.meta.out_dir.parent().unwrap(), &self.ex_dealer)?;
                }
                b"done" => {
                    trace!("[CLI EXP] done");
                    let reason = &msgb[2];
                    if reason == b"threshold" {
                        info!("Population reached fitness threshold. Ending experiment.");
                    }
                    self.ui_dealer.send("done", 0)?;
                    self.done = true;
                    return Ok(());
                }
                b"error" => {
                    let fatal = msgb[1][0] != 0;
                    let msg = str::from_utf8(&msgb[2])?;
                    if fatal {
                        bail!("[FATAL] From experiment thread: {}", msg);
                    } else {
                        error!("From experiment thread: {}", msg);
                    }
                }
                _ => (),
            }

            // If messages are arriving too fast, break loop to check for abort
            i += 1;
            if i >= 100 {
                break;
            }
        }

        Ok(())
    }

    fn process_ui_q(&mut self) -> Result<()> {
        if let Ok(msgb) = self.ui_dealer.recv_multipart(zmq::DONTWAIT) {
            let cmd = &msgb[0][..];
            match cmd {
                b"abort" => {
                    trace!("Sending abort to experiment thread");
                    self.ex_dealer.send("abort", 0)?;
                    debug!("Experiment thread aborted");
                    self.done = true;
                }
                b"detach" => {
                    trace!("Sending detach to experiment thread");
                    self.ex_dealer.send("detach", 0)?;
                    self.done = true;
                }
                _ => (),
            }
        }

        Ok(())
    }
}

fn handle_prog(msgb: &[Vec<u8>], out_dir: &Path) -> Result<()> {
    let prog_type = msgb[1].as_slice();

    if prog_type == b"gen" && msgb[3] == b"done" {
        let evaluation: PopEvaluation = serde_json::from_slice(&msgb[4])?;
        let path = out_dir.join("data/fitness.csv");
        let file = OpenOptions::new().append(true).open(path)?;
        let mut wtr = csv::Writer::from_writer(file);
        wtr.write_record([
            evaluation.avg_fitness.to_string(),
            evaluation.best_fitness.to_string(),
        ])?;
        wtr.flush()?;
        // info!(
        //     "Generation finished: Best {} Avg {}",
        //     evaluation.best_fitness, evaluation.avg_fitness
        // );
    }

    if prog_type == b"node" {
        let nd_id = de_u64(&msgb[2])?;
        let status_u8 = de_u8(&msgb[3])?;
        let status = NodeStatus::from(status_u8);
        match status {
            NodeStatus::Pulling => {
                info!("🐋 Node {:x} is pulling the docker image...", nd_id)
            }
            NodeStatus::Crashed => warn!("🐋 Node {:x} crashed", nd_id),
            NodeStatus::Active => {
                info!("🐋 Node {:x} is running your container...", nd_id)
            }
            NodeStatus::Idle => {
                info!("🐋 Node {:x} is loafing around...", nd_id)
            }
            NodeStatus::Noop => {
                warn!("🐋 Node {:x} sent an invalid status: {}", nd_id, status_u8);
            }
        }
    }

    Ok(())
}

fn handle_save_population(msgb: &[Vec<u8>], out_dir: &Path) -> Result<()> {
    let agents_dir = out_dir.join("agents");
    let agents: Vec<String> =
        serde_json::from_slice(&msgb[3]).context("failed to deserialize agents")?;
    for (i, agent) in agents.into_iter().enumerate() {
        let path = agents_dir.join(format!("agent_{}.json", i));
        fs::write(path, agent)?;
    }

    Ok(())
}

fn handle_save_data(msgb: &[Vec<u8>], out_dir: &Path) -> Result<()> {
    let data_dir = out_dir.join("data");
    let gen_num = de_u32(&msgb[3])?;
    save_exp_data_b(&msgb[4], &data_dir, gen_num)?;

    Ok(())
}
