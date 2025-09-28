use core::str;
use std::{
    fs::{self, File},
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    sync::Arc,
    thread,
};

use super::ExpConfig;
use super::exp_manager::ExpManager;
use super::tui;
use crate::{
    ExperimentMeta, bail_assert,
    blueprint::{Blueprint, InitMethod},
    create_exp_dir, de_u32, docker, zft,
};
use anyhow::{Context, Result, bail};
use serde_json::Value;
use tracing::{debug, info};

pub struct Client {
    ctx: zmq::Context,
    broker_addr: String,
    broker_url: String,
}

impl Client {
    pub fn new(broker_addr: String) -> Self {
        let ctx = zmq::Context::new();
        let broker_url = format!("tcp://{}:5555", &broker_addr);
        Self {
            ctx,
            broker_addr,
            broker_url,
        }
    }

    /// Prepare a new experiment from a squid project directory
    ///
    /// Returns an experiment metadata object
    pub fn prepare(&self, project_path: PathBuf, config: &ExpConfig) -> Result<ExperimentMeta> {
        bail_assert!(docker::is_installed()?, "Docker must be installed");

        bail_assert!(
            project_path.join("squid.toml").exists(),
            "Invalid Squid project: No `squid.toml` found in {}",
            project_path.canonicalize()?.display()
        );
        bail_assert!(
            project_path.join("Dockerfile").exists(),
            "Invalid Squid project: No `Dockerfile` found in {}",
            project_path.canonicalize()?.display()
        );

        // Ping broker

        self.ping()?;

        // Read blueprint

        let bpath = project_path.join("squid.toml");
        let blueprint_s = fs::read_to_string(&bpath)
            .with_context(|| format!("Failed to open blueprint file `{}`", bpath.display()))?;
        let mut blueprint: Blueprint = toml::from_str(&blueprint_s)
            .with_context(|| format!("Failed to parse blueprint file `{}`", bpath.display()))?;
        info!("🔧 Read blueprint from `{}`", bpath.display());
        blueprint.validate()?;
        if config.test {
            blueprint.ga.num_generations = 1;
            blueprint.ga.population_size = config.test_threads;
            blueprint.ga.save_fraction = 1.0;
            blueprint.ga.fitness_threshold = None;
        }
        let blueprint_s = toml::to_string(&blueprint)?;

        // Build and push docker image

        info!("🐋 Building docker image...");
        let task_image = blueprint.experiment.image.as_str();
        if !docker::build(task_image, project_path.to_str().unwrap())?
            .wait()?
            .success()
        {
            // failure message
            bail!("Failed to build docker image");
        }
        if config.should_push && !task_image.starts_with("docker.io/library/") {
            info!("🐋 Pushing docker image...");
            if !docker::push(&task_image)?.wait()?.success() {
                // failure message
                bail!("Failed to push docker image to {}", task_image);
            }
        }

        // Generate experiment ID
        let id: u64 = rand::random();
        let id_x = format!("{:x}", id);

        // Create outdir

        let out_dir = project_path.join(&blueprint.experiment.out_dir);
        let exp_dir = out_dir.join(&id_x);
        create_exp_dir(&out_dir, &id_x, &blueprint)?;

        // Check for optional seeds

        let seeds_b = if let InitMethod::Seeded { ref dir, .. } = blueprint.ga.init_method {
            let seed_dir = project_path.join(dir);
            let population_size = blueprint.ga.population_size;
            let seeds = read_agents_from_dir(&seed_dir, population_size)?;
            info!(
                "🔧 Seeding population from `{}`",
                seed_dir.canonicalize()?.display()
            );
            bail_assert!(seeds.len() > 0, "Seeds directory was empty");
            serde_json::to_vec(&seeds)?
        } else {
            let seeds = Vec::<Vec<u8>>::new();
            serde_json::to_vec(&seeds)?
        };

        // Initialize experiment on broker

        let id_b = id.to_be_bytes();
        let broker_sock = self.ctx.socket(zmq::DEALER)?;
        broker_sock.set_identity(&id_b)?;
        broker_sock.connect(&self.broker_url)?;

        info!("🧪 Starting experiment {:x}", id);
        let blueprint_b = blueprint_s.as_bytes();
        broker_sock.send_multipart(["run".as_bytes(), &id_b, &blueprint_b, &seeds_b], 0)?;

        if broker_sock.poll(zmq::POLLIN, 5000)? == 0 {
            bail!("Timed out waiting for broker to send redirect port.");
        }

        let msgb = broker_sock.recv_multipart(0)?;
        let cmd = msgb[0].as_slice();
        let url = match cmd {
            b"redirect" => {
                let port = de_u32(&msgb[1])?;
                format!("tcp://{}:{}", &self.broker_addr, port)
            }
            b"error" => {
                let error = str::from_utf8(&msgb[1])?;
                bail!("[Broker] {}", error);
            }
            x => bail!(
                "Broker did not respond with redirect, sent {}",
                str::from_utf8(x)?
            ),
        };

        Ok(ExperimentMeta {
            id,
            blueprint,
            url,
            out_dir: exp_dir,
            is_new: true,
            is_local: config.local,
            is_test: config.test,
        })
    }

    pub fn attach(&self, id_s: &str, out_dir: &Path) -> Result<ExperimentMeta> {
        let id = u64::from_str_radix(id_s, 16)
            .context("Failed to parse provided experiment ID string")?;
        self.ping()?;

        let broker_sock = self.ctx.socket(zmq::DEALER)?;
        broker_sock.connect(&self.broker_url)?;

        broker_sock.send_multipart(["attach".as_bytes(), &id.to_be_bytes()], 0)?;

        if broker_sock.poll(zmq::POLLIN, 5000)? == 0 {
            bail!("Timed out waiting for broker to send redirect port.");
        }

        let msgb = broker_sock.recv_multipart(0)?;
        let cmd = msgb[0].as_slice();
        let (url, blueprint) = match cmd {
            b"reattach" => {
                let port = de_u32(&msgb[1])?;
                let blueprint = rmp_serde::from_slice(&msgb[2])?;
                let url = format!("tcp://{}:{}", &self.broker_addr, port);
                (url, blueprint)
            }
            b"error" => {
                let error = str::from_utf8(&msgb[1])?;
                bail!("[Broker] {}", error);
            }
            x => bail!(
                "Broker did not respond with redirect, sent {}",
                str::from_utf8(x)?
            ),
        };

        let exp_dir = out_dir.join(id_s);
        if !exp_dir.exists() {
            create_exp_dir(out_dir, id_s, &blueprint)?;
        }

        Ok(ExperimentMeta {
            id,
            blueprint,
            url,
            out_dir: exp_dir,
            is_new: false,
            is_local: false,
            is_test: false,
        })
    }

    /// Run the squid client with the optional TUI
    pub fn run(&self, experiment: Arc<ExperimentMeta>, show_tui: bool) -> Result<bool> {
        let ex_sock = self.ctx.socket(zmq::PAIR)?;
        ex_sock.bind("inproc://experiment")?;

        let ctx_clone = self.ctx.clone();
        let experiment_clone = Arc::clone(&experiment);
        let exp_thread = thread::spawn(move || -> bool {
            let mut em = ExpManager::new(experiment_clone, ctx_clone).unwrap();
            em.run()
        });

        if show_tui {
            let mut app = tui::App::new(experiment).context("TUI failed to start")?;
            app.run(ex_sock)?;
            debug!("App closed. Waiting for experiment thread")
        }

        let success = exp_thread.join().unwrap();
        Ok(success)
    }

    pub fn fetch(&self, id: &str, out_dir: &Path) -> Result<()> {
        let id =
            u64::from_str_radix(id, 16).context("Failed to parse provided experiment ID string")?;

        self.ping()?;

        let broker_sock = self.ctx.socket(zmq::DEALER)?;
        broker_sock.connect(&self.broker_url)?;

        broker_sock.send_multipart(["fetch".as_bytes(), &id.to_be_bytes()], 0)?;

        if broker_sock.poll(zmq::POLLIN, 5000)? == 0 {
            bail!("Timed out waiting for zft transmitter");
        }

        let msgb = broker_sock.recv_multipart(0)?;
        let cmd = msgb[0].as_slice();
        match cmd {
            b"zft" => {
                let port = de_u32(&msgb[1])?;
                let zft_url = format!("tcp://{}:{}", &self.broker_addr, port);
                let rcvsock = self.ctx.socket(zmq::DEALER)?;
                rcvsock.connect(&zft_url)?;
                zft::recv(out_dir, &rcvsock)?;
            }
            b"error" => {
                let msg = str::from_utf8(&msgb[1])?;
                bail!("[Broker] {}", msg);
            }
            x => bail!(
                "Received invalid response from broker: {}",
                str::from_utf8(x)?
            ),
        }

        Ok(())
    }

    pub fn validate(&self, experiment: &ExperimentMeta) -> Result<bool> {
        println!("Validating csv data...");

        let data_dir = experiment.out_dir.join("data");
        let mut success = true;

        for csv in fs::read_dir(data_dir)? {
            let path = csv?.path();
            if path.extension().and_then(|s| s.to_str()) != Some("csv") {
                continue;
            }
            let file_name = path.file_name().unwrap();
            let file = fs::File::open(&path)?;
            let reader = BufReader::new(file);
            let mut lines = reader
                .lines()
                .map(|l| l.unwrap().chars().filter(|c| *c == ',').count()); // not counting gen column

            let num_headers = lines.next().unwrap();
            let Some(num_fields) = lines.next() else {
                println!("  ✘ {:?} is empty", &file_name);
                success = false;
                continue;
            };

            if num_fields != num_headers {
                println!(
                    "  ✘ {:?} is malformed: had {} headers but {} fields",
                    &file_name, num_headers, num_fields
                );
                success = false;
                continue;
            }

            println!("  ✔ {:?}", &file_name);
        }

        Ok(success)
    }

    /// Ping broker for availability and version compatibility check
    fn ping(&self) -> Result<()> {
        let temp_sock = self.ctx.socket(zmq::DEALER)?;
        temp_sock.set_linger(0)?;
        temp_sock.connect(&self.broker_url)?;
        temp_sock.send_multipart(
            [
                "ping",
                env!("CARGO_PKG_VERSION_MAJOR"),
                env!("CARGO_PKG_VERSION_MINOR"),
            ],
            0,
        )?;

        if temp_sock.poll(zmq::POLLIN, 5000)? == 0 {
            bail!("Squid broker was unresponsive at {}", self.broker_url);
        }

        let msgb = temp_sock.recv_multipart(0)?;
        let cmd = msgb[0].as_slice();
        match cmd {
            b"pong" => info!("🦑 Connected to Squid broker at {}", self.broker_url),
            b"pang" => {
                let broker_version = str::from_utf8(&msgb[1])?;
                bail!(
                    "Squid version mismatch (client={} broker={}) ... update required",
                    env!("CARGO_PKG_VERSION"),
                    broker_version
                );
            }
            x => bail!("Squid broker responded to ping with {}", str::from_utf8(x)?),
        }

        Ok(())
    }
}

fn read_agents_from_dir(dir: &Path, population_size: usize) -> Result<Vec<Vec<u8>>> {
    let entries = fs::read_dir(dir)
        .with_context(|| format!("Failed to read directory: {}", dir.display()))?;
    let mut agents = Vec::with_capacity(population_size);
    for (i, entry) in entries.enumerate() {
        if i >= population_size {
            break;
        }

        let entry = entry
            .with_context(|| format!("Failed to read directory entry at `{}`", dir.display()))?;
        let path = entry.path();
        if !path.is_file() {
            bail!("Found non-file entry: `{}`", path.display());
        }

        // Validate and minify the json
        let file = File::open(&path)
            .with_context(|| format!("Failed to open file: `{}`", path.display()))?;
        let json: Value = serde_json::from_reader(file)
            .with_context(|| format!("Failed to parse json from file: `{}`", path.display()))?;
        let content = serde_json::to_vec(&json)?;
        agents.push(content);
    }

    Ok(agents)
}
