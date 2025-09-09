mod tui;

use core::str;
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use crate::{
    Experiment, NodeStatus, PopEvaluation, bail_assert, bincode_deserialize,
    blueprint::{Blueprint, InitMethod},
    de_u8, de_u32, de_u64, de_usize, docker, zft,
};
use anyhow::{Context, Result, bail};
use log::warn;
use serde_json::Value;
use tracing::{debug, error, info};

struct Client {
    ctx: zmq::Context,
    broker_addr: String,
    broker_url: String,
}

impl Client {
    fn new(ctx: zmq::Context, broker_addr: String) -> Self {
        let broker_url = format!("tcp://{}:5555", &broker_addr);
        Self {
            ctx,
            broker_addr,
            broker_url,
        }
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

    /// Prepare a new experiment from a squid project directory
    ///
    /// Returns an experiment metadata object
    fn prepare(&self, project_path: PathBuf, test_threads: Option<usize>) -> Result<Experiment> {
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

        let is_local = self.broker_addr == "localhost";

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
        if let Some(t) = test_threads {
            blueprint.ga.num_generations = 1;
            blueprint.ga.population_size = t;
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
        if !is_local && !task_image.starts_with("docker.io/library/") {
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

        let mut out_dir = project_path.join(&blueprint.experiment.out_dir);
        if !out_dir.exists() {
            fs::create_dir_all(&out_dir)?;
        }
        // let timestamp = Local::now().format("%Y-%m-%d_%H-%M-%S").to_string();
        // out_dir.push(timestamp);
        out_dir.push(&id_x);
        fs::create_dir(&out_dir)?;
        fs::write(out_dir.join("EXPERIMENT_ID"), &id_x)?;
        fs::create_dir(out_dir.join("agents"))?;
        fs::write(
            out_dir.join("population_parameters.txt"),
            format!("{:#?}", &blueprint.ga),
        )?;

        let data_dir = out_dir.join("data");
        fs::create_dir(&data_dir)?;
        fs::write(data_dir.join("fitness.csv"), "avg,best\n")?;

        if let Some(csv_data) = &blueprint.csv_data {
            for (name, headers) in csv_data {
                fs::write(
                    data_dir.join(name).with_extension("csv"),
                    format!("gen,{}\n", headers.join(",")),
                )?;
            }
        }

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

        Ok(Experiment {
            id,
            blueprint,
            url,
            out_dir,
            is_new: true,
            is_local,
            is_test: test_threads.is_some(),
        })
    }

    fn attach(&self, id: &str, out_dir: &Path) -> Result<Experiment> {
        let id =
            u64::from_str_radix(id, 16).context("Failed to parse provided experiment ID string")?;
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
                let blueprint = bincode_deserialize(&msgb[2])?;
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

        // needs to send back blueprint & port

        Ok(Experiment {
            id,
            blueprint,
            url,
            out_dir: out_dir.to_path_buf(),
            is_new: false,
            is_local: false,
            is_test: false,
        })
    }

    /// Run the squid client with the optional TUI
    fn run(&self, experiment: Arc<Experiment>, show_tui: bool) -> Result<bool> {
        let ex_sock = self.ctx.socket(zmq::PAIR)?;
        ex_sock.bind("inproc://experiment")?;
        let ctx_clone = self.ctx.clone();
        let experiment_clone = Arc::clone(&experiment);
        let exp_thread = thread::spawn(move || -> bool {
            let ctx = ctx_clone;
            let experiment = experiment_clone;
            let ui_sock = ctx.socket(zmq::PAIR).unwrap();
            ui_sock.connect("inproc://experiment").unwrap();
            match (|| -> Result<bool> {
                let id_b = experiment.id.to_be_bytes();
                let broker_sock = ctx.socket(zmq::DEALER)?;
                broker_sock.set_identity(&id_b)?;
                broker_sock.connect(&experiment.url)?;

                info!(
                    "🔗 Experiment DEALER socket connected to {}",
                    &experiment.url
                );

                if experiment.is_new {
                    broker_sock.send_multipart(["run".as_bytes(), &id_b], 0)?;
                }

                let mut sockets = [
                    broker_sock.as_poll_item(zmq::POLLIN),
                    ui_sock.as_poll_item(zmq::POLLIN),
                ];

                const HB_INTERVAL: Duration = Duration::from_secs(1);
                let mut last_hb = Instant::now();

                'o: loop {
                    if last_hb.elapsed() >= HB_INTERVAL {
                        broker_sock.send("hb", 0)?;
                        last_hb = Instant::now();
                    }

                    let timeout = HB_INTERVAL
                        .checked_sub(last_hb.elapsed())
                        .map(|x| x.as_millis() as i64 + 1)
                        .unwrap_or(1);

                    if zmq::poll(&mut sockets, timeout)? == 0 {
                        continue;
                    }

                    let mut i = 0;
                    while let Ok(msgb) = broker_sock.recv_multipart(zmq::DONTWAIT) {
                        let cmd = msgb[0].as_slice();
                        match cmd {
                            b"prog" => {
                                ui_sock.send_multipart(&msgb[2..], 0)?;
                                handle_prog(&msgb, &experiment.out_dir)?;
                            }
                            b"save" => {
                                debug!("[CLI EXP] save");
                                let save_type = msgb[2].as_slice();
                                match save_type {
                                    b"population" => {
                                        handle_save_population(&msgb, &experiment.out_dir)?
                                    }
                                    b"data" => handle_save_data(&msgb, &experiment.out_dir)?,
                                    _ => (),
                                }
                            }
                            b"done" => {
                                debug!("[CLI EXP] done");
                                let reason = &msgb[2];
                                if reason == b"threshold" {
                                    info!(
                                        "Population reached fitness threshold. Ending experiment."
                                    );
                                }
                                ui_sock.send("done", 0)?;
                                break 'o;
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

                    if sockets[1].is_readable() {
                        let msg = ui_sock.recv_bytes(0)?;
                        if msg == b"abort" {
                            debug!("Sending abort to experiment thread");
                            broker_sock.send_multipart([b"abort", id_b.as_slice()], 0)?;
                            debug!("Experiment thread aborted");
                            break;
                        }
                    }
                }

                Ok(true)
            })() {
                Ok(s) => s,
                Err(e) => {
                    error!("{}", e.to_string());
                    ui_sock.send("crashed", 0).unwrap();
                    false
                }
            }
        });

        if show_tui {
            let mut app = tui::App::new(experiment).context("TUI failed to start")?;
            app.run(ex_sock)?;
            debug!("App closed. Waiting for experiment thread")
        }

        let success = exp_thread.join().unwrap();

        if success {
            info!("🧪 Experiment done");
        }

        Ok(success)
    }

    fn fetch(&self, id: &str, out_dir: &Path) -> Result<()> {
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

    fn validate(&self, experiment: &Experiment) -> Result<bool> {
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
}

pub fn run(
    path: PathBuf,
    broker_addr: String,
    test_threads: Option<usize>,
    show_tui: bool,
) -> Result<bool> {
    let ctx = zmq::Context::new();
    let client = Client::new(ctx, broker_addr);
    let experiment = client.prepare(path, test_threads)?;
    let experiment = Arc::new(experiment);
    let mut success = client.run(Arc::clone(&experiment), show_tui)?;

    if experiment.is_test {
        success = client.validate(&experiment)?;
    }

    Ok(success)
}

pub fn attach(broker_addr: String, id: String, out_dir: &Path, show_tui: bool) -> Result<bool> {
    let ctx = zmq::Context::new();
    let client = Client::new(ctx, broker_addr);
    let experiment = client.attach(&id, out_dir)?;
    let experiment = Arc::new(experiment);
    let success = client.run(Arc::clone(&experiment), show_tui)?;

    Ok(success)
}

pub fn fetch(broker_addr: String, id: String, out_dir: &Path) -> Result<()> {
    let ctx = zmq::Context::new();
    let client = Client::new(ctx, broker_addr);
    client.fetch(&id, out_dir)?;

    Ok(())
}

fn handle_prog(msgb: &[Vec<u8>], out_dir: &Path) -> Result<()> {
    let prog_type = msgb[2].as_slice();

    if prog_type == b"gen" && msgb[4] == b"done" {
        let evaluation: PopEvaluation = serde_json::from_slice(&msgb[5])?;
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
        let nd_id = de_u64(&msgb[3])?;
        let status_u8 = de_u8(&msgb[4])?;
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
    let gen_num = de_usize(&msgb[3])?;
    let data: Option<HashMap<String, Vec<Vec<Option<f64>>>>> = serde_json::from_slice(&msgb[4])
        .context("serde_json failed to parse additional sim data")?;
    if let Some(data) = data {
        for (name, rows) in data {
            let path = data_dir.join(&name).with_extension("csv");
            let file = OpenOptions::new().append(true).open(path)?;
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

    Ok(())
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
