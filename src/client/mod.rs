mod tui;

use core::str;
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader},
    ops::ControlFlow,
    path::Path,
    thread,
};

use crate::{
    bail_assert,
    util::{
        PopEvaluation,
        blueprint::{Blueprint, InitMethod},
        de_u32, de_usize, docker,
    },
};
use anyhow::{Context, Result, bail};
use chrono::Local;
use serde_json::Value;
use tracing::{debug, error, info};

pub fn run(
    path: &Path,
    broker_addr: String,
    test_threads: Option<usize>,
    tui: bool,
) -> Result<bool> {
    bail_assert!(docker::is_installed()?, "Docker must be installed");

    let bpath = path.join("squid.toml");
    bail_assert!(
        path.join("squid.toml").exists(),
        "Invalid Squid project: No `squid.toml` found in {}",
        path.canonicalize()?.display()
    );
    bail_assert!(
        path.join("Dockerfile").exists(),
        "Invalid Squid project: No `Dockerfile` found in {}",
        path.canonicalize()?.display()
    );

    let ctx = zmq::Context::new();
    let broker_url = format!("tcp://{}:5555", &broker_addr);

    // Ping broker

    if let Err(e) = ping_broker(&ctx, &broker_url) {
        error!("{}", e.to_string());
        return Ok(false);
    }

    // Read blueprint

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
    }
    let blueprint_s = toml::to_string(&blueprint)?;

    // Build and push docker image

    info!("🐋 Building docker image...");
    let task_image = blueprint.experiment.image.as_str();
    if !docker::build(task_image, path.to_str().unwrap())?.success() {
        // failure message
        bail!("Failed to build docker image");
    }
    if !task_image.starts_with("docker.io/library/") {
        info!("🐋 Pushing docker image...");
        if !docker::push(&task_image)?.success() {
            // failure message
            bail!("Failed to push docker image to {}", task_image);
        }
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
        format!("{:#?}", &blueprint.ga),
    )?;

    let data_dir = out_dir.join("data");
    fs::create_dir(&data_dir)?;
    fs::write(data_dir.join("fitness.csv"), "avg,best\n")?;

    if let Some(ref csv_data) = blueprint.csv_data {
        for (name, headers) in csv_data {
            fs::write(
                data_dir.join(name).with_extension("csv"),
                format!("gen,{}\n", headers.join(",")),
            )?;
        }
    }

    // Check for optional seeds

    let seeds_b = if let InitMethod::Seeded { ref dir, .. } = blueprint.ga.init_method {
        let seed_dir = path.join(dir);
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

    // Start experiment in thread

    let id: u64 = rand::random();
    let out_dir_clone = out_dir.clone();

    let ex_sock = ctx.socket(zmq::PAIR)?;
    ex_sock.bind("inproc://experiment")?;
    let exp_thread = thread::spawn(move || -> Result<()> {
        let id_b = id.to_be_bytes();
        let ui_sock = ctx.socket(zmq::PAIR)?;
        ui_sock.connect("inproc://experiment")?;
        let broker_sock = ctx.socket(zmq::DEALER)?;
        broker_sock.set_identity(&id_b)?;
        broker_sock.connect(&broker_url)?;

        info!("🧪 Starting experiment {:x}", id);
        let blueprint_b = blueprint_s.as_bytes();
        broker_sock.send_multipart(["run".as_bytes(), &id_b, &blueprint_b, &seeds_b], 0)?;

        if broker_sock.poll(zmq::POLLIN, 5000)? > 0 {
            let msgb = broker_sock.recv_multipart(0)?;
            if msgb[0] == b"redirect" {
                let port = de_u32(&msgb[1])?;
                let exp_url = format!("tcp://{}:{}", &broker_addr, port);
                broker_sock.disconnect(&broker_url)?;
                broker_sock.connect(&exp_url)?;
                info!("🔗 Experiment DEALER socket connected to {}", &exp_url);

                broker_sock.send_multipart(["run".as_bytes(), &id_b], 0)?;
            } else {
                ui_sock.send("crashed", 0)?;
                bail!(
                    "Broker did not respond with redirect, sent {}",
                    str::from_utf8(&msgb[0])?
                );
            }
        } else {
            ui_sock.send("crashed", 0)?;
            bail!("Timed out waiting for broker to send redirect port.");
        }

        let mut sockets = [
            broker_sock.as_poll_item(zmq::POLLIN),
            ui_sock.as_poll_item(zmq::POLLIN),
        ];
        loop {
            match exp_loop(&mut sockets, &broker_sock, &ui_sock, &id_b, &out_dir_clone) {
                Ok(ControlFlow::Break(_)) => break,
                Ok(ControlFlow::Continue(_)) => continue,
                Err(e) => {
                    ui_sock.send("crashed", 0)?;
                    bail!("Exp loop error: {:#}", e);
                }
            }
        }

        Ok(())
    });

    if tui {
        let mut app = tui::App::new(&blueprint, id).context("TUI failed to start")?;
        app.run(ex_sock)?;
    }

    let mut success = match exp_thread.join().unwrap() {
        Ok(()) => {
            info!("🧪 Experiment done");
            true
        }
        Err(e) => {
            error!("{}", e.to_string());
            false
        }
    };

    if test_threads.is_some() && success {
        println!("Validating csv data...");

        let data_dir = out_dir.join("data");

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
    }

    Ok(success)
}

fn exp_loop(
    sockets: &mut [zmq::PollItem; 2],
    ex_dealer: &zmq::Socket,
    ui_sock: &zmq::Socket,
    id_b: &[u8],
    out_dir: &Path,
) -> Result<ControlFlow<()>> {
    zmq::poll(sockets, -1)?;

    let mut i = 0;
    while let Ok(msgb) = ex_dealer.recv_multipart(zmq::DONTWAIT) {
        let cmd = msgb[0].as_slice();
        match cmd {
            b"prog" => {
                ui_sock.send_multipart(&msgb[2..], 0)?;

                if msgb[2] == b"gen" && msgb[4] == b"done" {
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
                        let data: Option<HashMap<String, Vec<Vec<Option<f64>>>>> =
                            serde_json::from_slice(&msgb[4])
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
                    }
                    _ => (),
                }
            }
            b"done" => {
                ui_sock.send("done", 0)?;
                return Ok(ControlFlow::Break(()));
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
            ex_dealer.send_multipart([b"abort", id_b], 0)?;
            debug!("Experiment thread aborted");
            return Ok(ControlFlow::Break(()));
        }
    }

    Ok(ControlFlow::Continue(()))
}

fn ping_broker(ctx: &zmq::Context, broker_url: &str) -> Result<()> {
    let temp_sock = ctx.socket(zmq::DEALER)?;
    temp_sock.set_linger(0)?;
    temp_sock.connect(broker_url)?;
    temp_sock.send("ping", 0)?;
    if temp_sock.poll(zmq::POLLIN, 5000)? > 0 {
        let msgb = temp_sock.recv_bytes(0)?;
        if msgb == b"pong" {
            info!("🦑 Connected to Squid broker at {}", broker_url);
        } else {
            bail!(
                "Squid broker responded to ping with {}",
                str::from_utf8(&msgb)?
            );
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
