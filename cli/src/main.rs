mod logger;
mod viz;

use core::str;
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    ops::ControlFlow,
    path::{Path, PathBuf},
    thread,
};

use anyhow::{Context, Result, bail};
use chrono::Local;
use clap::{Parser, Subcommand};
use log::{error, info};
use serde_json::Value;
use shared::{Blueprint, PopEvaluation, bail_assert, de_usize, docker, env};
use viz::App;

#[derive(Parser)]
#[command(about = "Squid Client CLI")]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new squid experiment in the current directory, or in a new one if a path is specified
    Init {
        /// Path to initialize in
        #[arg(default_value = ".")]
        path: PathBuf,
    },
    /// Run an experiment
    Run {
        /// Path to squid project directory
        #[arg(default_value = ".")]
        path: PathBuf,
        // /// Run in test mode (spawns a single worker container locally and simulates a single agent)
        // #[arg(short, long)]
        // test: bool,
    },
    /// Abort the running experiment (deprecated)
    Abort,
    // {
    //     /// Experiment id, printed after calling `squid run`
    //     // id: String,
    // },
    /// Validate a blueprint file
    Validate {
        /// Blueprint file
        #[arg(short, long, value_name = "FILE", default_value = "./blueprint.toml")]
        blueprint: PathBuf,
    },
}

fn main() -> Result<()> {
    let args = Args::parse();

    match &args.command {
        Commands::Init { path } => {
            fs::create_dir_all(path)?;
            fs::write(
                path.join("blueprint.toml"),
                include_bytes!("../templates/blueprint.toml"),
            )?;
            fs::write(
                path.join(".env"),
                include_bytes!("../templates/template.env"),
            )?;
            fs::write(
                path.join("Dockerfile"),
                include_bytes!("../templates/Dockerfile"),
            )?;
            fs::write(path.join("requirements.txt"), b"")?;
            fs::write(
                path.join(".gitignore"),
                include_bytes!("../templates/template.gitignore"),
            )?;
            let sim_dir = path.join("simulation");
            fs::create_dir(&sim_dir)?;
            fs::write(
                sim_dir.join("main.py"),
                include_bytes!("../templates/simulation/main.py"),
            )?;

            println!(
                "🦑 Initialized Squid project in {}",
                path.canonicalize()?.display()
            );
        }
        Commands::Run { path } => {
            bail_assert!(docker::is_installed()?, "Docker must be installed");

            let bpath = path.join("blueprint.toml");
            bail_assert!(
                path.join("blueprint.toml").exists(),
                "Invalid Squid project: No `blueprint.toml` found in {}",
                path.canonicalize()?.display()
            );
            bail_assert!(
                path.join("Dockerfile").exists(),
                "Invalid Squid project: No `Dockerfile` found in {}",
                path.canonicalize()?.display()
            );

            let ctx = zmq::Context::new();
            let logs = logger::init();
            let broker_url = env("SQUID_BROKER_URL")? + ":5555";

            // Ping broker

            if let Err(e) = ping_broker(&ctx, &broker_url) {
                eprintln!("{}", e.to_string());
                return Ok(());
            }

            // Read blueprint

            let blueprint_s = fs::read_to_string(&bpath)
                .with_context(|| format!("Failed to open blueprint file `{}`", bpath.display()))?;
            let blueprint: Blueprint = toml::from_str(&blueprint_s)
                .with_context(|| format!("Failed to parse blueprint file `{}`", bpath.display()))?;
            let blueprint_b = fs::read(&bpath).expect("Blueprint file already validated");
            info!("🔧 Read blueprint from `{}`", bpath.display());
            blueprint.validate()?;

            // Build and push docker image

            let task_image = blueprint.experiment.task_image.as_str();
            if !docker::build(task_image, path.to_str().unwrap())?.success() {
                // failure message
                bail!("Failed to build docker image");
            }
            if !task_image.starts_with("docker.io/library/")
                && !docker::push(&task_image)?.success()
            {
                // failure message
                bail!("Failed to push docker image to {}", task_image);
            }

            // Create outdir

            let mut out_dir = path.join(&blueprint.experiment.out_dir);
            if !out_dir.exists() {
                fs::create_dir_all(&out_dir)?;
            }
            let timestamp = Local::now().format("%Y-%m-%d_%H-%M-%S").to_string();
            out_dir.push(timestamp);
            fs::create_dir(&out_dir)?;
            fs::create_dir(out_dir.join("agents"))?;
            fs::write(
                out_dir.join("population_parameters.txt"),
                blueprint.ga.display(),
            )?;

            let data_dir = out_dir.join("data");
            fs::create_dir(&data_dir)?;
            fs::write(data_dir.join("fitness_scores.csv"), "avg,best\n")?;

            if let Some(ref csv_data) = blueprint.csv_data {
                for (name, headers) in csv_data {
                    fs::write(
                        data_dir.join(name).with_extension("csv"),
                        format!("gen,{}\n", headers.join(",")),
                    )?;
                }
            }

            // Check for optional seeds

            let seeds_b = match blueprint.experiment.seed_dir {
                Some(ref seed_dir) => {
                    let seed_dir = path.join(seed_dir);
                    let population_size = blueprint.ga.population_size;
                    let seeds = read_agents_from_dir(&seed_dir, population_size)?;
                    info!(
                        "🔧 Seeding population from `{}`",
                        seed_dir.canonicalize()?.display()
                    );
                    serde_json::to_vec(&seeds)?
                }
                None => {
                    let seeds = Vec::<Vec<u8>>::with_capacity(0);
                    serde_json::to_vec(&seeds)?
                }
            };

            // Start TUI

            let id: u64 = rand::random();
            let mut app = App::new(&blueprint, id, logs).context("TUI failed to start")?;

            // Start experiment in thread

            let bthread_sock = ctx.socket(zmq::PAIR)?;
            bthread_sock.bind("inproc://broker_loop")?;
            let bthread = thread::spawn(move || -> Result<()> {
                let id_b = id.to_be_bytes();
                let tui_sock = ctx.socket(zmq::PAIR)?;
                tui_sock.connect("inproc://broker_loop")?;
                let broker_sock = ctx.socket(zmq::DEALER)?;
                broker_sock.set_identity(&id_b)?;
                broker_sock.connect(&broker_url)?;

                info!("🧪 Starting experiment {:x}", id);
                broker_sock.send_multipart(["run".as_bytes(), &id_b, &blueprint_b, &seeds_b], 0)?;

                let mut sockets = [
                    broker_sock.as_poll_item(zmq::POLLIN),
                    tui_sock.as_poll_item(zmq::POLLIN),
                ];
                loop {
                    match broker_loop(&mut sockets, &broker_sock, &tui_sock, &id_b, &out_dir) {
                        Ok(ControlFlow::Break(_)) => break,
                        Ok(ControlFlow::Continue(_)) => continue,
                        Err(e) => {
                            error!("Broker loop error: {:#}", e);
                            tui_sock.send("crashed", 0)?;
                            break;
                        }
                    }
                }

                Ok(())
            });

            app.run(&bthread_sock, &bthread)?;

            bthread.join().unwrap()?;
            info!("🧪 Experiment done");
        }
        Commands::Abort => {
            let ctx = zmq::Context::new();
            let broker_url = env("SQUID_BROKER_URL")? + ":5555";
            // Abort cmd is temporary so just blocking and assuming it will connect
            let broker_sock = ctx.socket(zmq::DEALER)?;
            broker_sock.connect(&broker_url)?;
            broker_sock.send("abort", 0)?;
            println!("abort signal sent, check other terminal");
        }
        Commands::Validate { blueprint: bpath } => {
            let blueprint_s = fs::read_to_string(bpath)
                .with_context(|| format!("Failed to open blueprint file `{}`", bpath.display()))?;
            let blueprint: Blueprint = toml::from_str(&blueprint_s)
                .with_context(|| format!("Failed to parse blueprint file `{}`", bpath.display()))?;
            blueprint.validate()?;
            println!("✅ Validated blueprint from `{}`", bpath.display());
        }
    }

    Ok(())
}

fn broker_loop(
    sockets: &mut [zmq::PollItem; 2],
    broker_sock: &zmq::Socket,
    tui_sock: &zmq::Socket,
    id_b: &[u8],
    out_dir: &Path,
) -> Result<ControlFlow<()>> {
    zmq::poll(sockets, -1)?;

    if sockets[0].is_readable() {
        let msgb = broker_sock.recv_multipart(0)?;
        let cmd = msgb[0].as_slice();
        match cmd {
            b"prog" => {
                tui_sock.send_multipart(&msgb[2..], 0)?;

                if msgb[2] == b"gen" && msgb[4] == b"done" {
                    let evaluation: PopEvaluation = serde_json::from_slice(&msgb[5])?;
                    let path = out_dir.join("data/fitness_scores.csv");
                    let file = OpenOptions::new().append(true).open(path)?;
                    let mut wtr = csv::Writer::from_writer(file);
                    wtr.write_record([
                        evaluation.avg_fitness.to_string(),
                        evaluation.best_fitness.to_string(),
                    ])?;
                    wtr.flush()?;
                }
            }
            b"save" => {
                let save_type = msgb[2].as_slice();
                match save_type {
                    b"population" => {
                        let agents_dir = out_dir.join("agents");
                        let agents: Vec<String> = serde_json::from_slice(&msgb[3])
                            .context("failed to deserialize agents")?;
                        for (i, agent) in agents.into_iter().enumerate() {
                            let path = agents_dir.join(format!("agent_{}.json", i));
                            fs::write(path, agent)?;
                        }
                    }
                    b"data" => {
                        let data_dir = out_dir.join("data");
                        let gen_num = de_usize(&msgb[3])?;
                        let data: HashMap<String, Vec<Vec<Option<f64>>>> =
                            serde_json::from_slice(&msgb[4])
                                .context("serde_json failed to parse additional sim data")?;
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
                    _ => (),
                }
            }
            b"done" => {
                tui_sock.send("done", 0)?;
                return Ok(ControlFlow::Break(()));
            }
            b"error" => {
                let msg = str::from_utf8(&msgb[1])?;
                error!("Broker error: {}", msg);
                tui_sock.send("crashed", 0)?;
                return Ok(ControlFlow::Break(()));
                // TODO: determine if fatal and recover?
            }
            _ => (),
        }
    }

    if sockets[1].is_readable() {
        let msg = tui_sock.recv_bytes(0)?;
        if msg == b"abort" {
            broker_sock.send_multipart([b"abort", id_b], 0)?;
        }
    }

    Ok(ControlFlow::Continue(()))
}

fn ping_broker(ctx: &zmq::Context, broker_url: &str) -> Result<()> {
    let temp_sock = ctx.socket(zmq::DEALER)?;
    temp_sock.set_linger(0)?;
    temp_sock.connect(broker_url)?;
    temp_sock.send("status", 0)?;
    if temp_sock.poll(zmq::POLLIN, 5000)? > 0 {
        let msgb = temp_sock.recv_multipart(0)?;
        let status = msgb[1].as_slice();
        match status {
            b"idle" => info!("🔗 Connected to Squid broker at {}", broker_url),
            b"busy" => bail!(
                "Squid broker is busy with another experiment at {}",
                broker_url
            ),
            x => bail!(
                "Squid broker sent invalid status ({}) from {}",
                str::from_utf8(x)?,
                broker_url
            ),
        }
    } else {
        bail!("Squid broker was unresponsive at {}", broker_url);
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
