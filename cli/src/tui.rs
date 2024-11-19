#![allow(unused)]
use core::str;
use std::{
    io::Stdout,
    time::{Duration, Instant},
};

use anyhow::{bail, Result};
use shared::{de_usize, Blueprint};

#[derive(Debug, Clone)]
pub enum AgentState {
    Queued,
    Running { start: Instant },
    Failed { elapsed: Duration, msg: String },
    Done { elapsed: Duration },
}

pub struct Tracker {
    cur_gen: usize,
    num_gens: usize,
    agents: Vec<AgentState>,
    stdout: Stdout,
}

impl Tracker {
    pub fn new(blueprint: &Blueprint) -> Self {
        let agents = vec![AgentState::Queued; blueprint.ga_config.population_size];

        Self {
            cur_gen: 0,
            num_gens: blueprint.ga_config.num_generations,
            agents: vec![AgentState::Queued; blueprint.ga_config.population_size],
            stdout: std::io::stdout(),
        }
    }

    pub fn handle_progress(&mut self, msgb: &[Vec<u8>]) -> Result<()> {
        let prog_type = str::from_utf8(&msgb[0])?;
        match prog_type {
            "gen" => {
                // let new_gen = str::from_utf8(&msgb[1])?;
                // self.cur_gen = new_gen;
                // let gen_num = println!("🧫 Running generation {}", i + 1);
            }
            "agent" => {
                let ix = de_usize(&msgb[1])?;
                let status = &self.agents[ix];
                let new_status = str::from_utf8(&msgb[2])?;
                self.agents[ix] = match new_status {
                    "running" => AgentState::Running {
                        start: Instant::now(),
                    },
                    "done" => {
                        let AgentState::Running { start } = status else {
                            bail!("Received DONE status but agent was not in RUNNING state");
                        };

                        AgentState::Done {
                            elapsed: start.elapsed(),
                        }
                    }
                    "failed" => {
                        let AgentState::Running { start } = status else {
                            bail!("Received FAILED status but agent was not in RUNNING state");
                        };
                        let msg = String::from_utf8(msgb[3].clone())?;

                        AgentState::Failed {
                            elapsed: start.elapsed(),
                            msg,
                        }
                    }
                    x => bail!("Received invalid status: {}", x),
                };

                println!("progress {} {} {}", prog_type, ix, new_status);
            }
            x => bail!("Received invalid progress type: {}", x),
        }

        Ok(())
    }
}

pub fn handle_progress(msgb: &[Vec<u8>]) -> Result<()> {
    let prog_type = str::from_utf8(&msgb[0])?;
    match prog_type {
        "gen" => {
            let new_gen = de_usize(&msgb[1])?;
            println!("🧫 Running generation {}", new_gen);
        }
        "agent" => {
            let ix = de_usize(&msgb[1])?;
            let status = str::from_utf8(&msgb[2])?;
            println!("{} {} {}", prog_type, ix, status);
        }
        x => bail!("Received invalid progress type: {}", x),
    }

    Ok(())
}
