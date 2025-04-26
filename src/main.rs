mod broker;
mod client;
mod node;
mod util;

use core::str;
use std::{fs, path::PathBuf, thread};

use crate::util::blueprint::Blueprint;
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tracing::{error_span, level_filters::LevelFilter};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use util::stdout_buffer::StdoutBuffer;

#[derive(Parser)]
#[clap(version)]
#[command(about = "🦑 Squid - Distributed Neuroevolution 🦑")]
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
    /// Run an experiment via the squid client
    Run {
        /// Path to squid project directory
        #[arg(default_value = ".")]
        path: PathBuf,
        /// Run in local mode - spawns a squid broker and node locally and runs the experiment on your device
        #[arg(short, long)]
        local: bool,
        /// If running in local mode, specifies number of worker threads to spawn (default: use all available cores)
        #[arg(short, long)]
        num_threads: Option<usize>,
        /// Run in test mode - like local mode but spawns only one worker and quits after one agent
        #[arg(short, long)]
        test: bool,
        /// Do not run the TUI (terminal user interface)
        #[arg(long)]
        no_tui: bool,
    },
    /// Run the squid broker
    Broker {
        /// Run for one experiment, then exit
        #[arg(short('1'), long)]
        once: bool,
    },
    /// Run the squid node
    Node {
        /// Number of worker threads to spawn, defaults to use all available cores
        #[arg(short, long)]
        num_threads: Option<usize>,
        /// For testing only. Run in local mode, assume any task image passed is already in the local docker library.
        #[arg(short, long)]
        local: bool,
    },
    /// Validate a blueprint file
    Validate {
        /// Blueprint file
        #[arg(default_value = "squid.toml")]
        blueprint: PathBuf,
    },
    // /// Abort the running experiment (deprecated)
    // Abort,
    // {
    //     /// Experiment id, printed after calling `squid run`
    //     // id: String,
    // },
}

fn main() -> Result<()> {
    let args = Args::parse();

    match args.command {
        Commands::Init { path } => {
            fs::create_dir_all(&path)?;
            fs::write(
                path.join("squid.toml"),
                include_bytes!("../templates/squid.toml"),
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
        Commands::Run {
            path,
            local,
            num_threads,
            test,
            no_tui,
        } => {
            let mut stdout_buffer = None;
            if test || no_tui {
                let level = if test {
                    LevelFilter::DEBUG
                } else {
                    LevelFilter::INFO
                };
                tracing_subscriber::fmt()
                    .with_max_level(level)
                    .with_target(false)
                    .init();
            } else {
                stdout_buffer = Some(StdoutBuffer::new());
                tracing_subscriber::registry()
                    .with(
                        tracing_subscriber::fmt::layer()
                            .with_target(false)
                            .with_writer(stdout_buffer.clone().unwrap()),
                    )
                    .with(tui_logger::TuiTracingSubscriberLayer)
                    .init();
                tui_logger::init_logger(tui_logger::LevelFilter::Info).unwrap();
            }

            if test {
                let broker_thread =
                    thread::spawn(|| error_span!("broker").in_scope(|| broker::run(true)));
                let node_thread = thread::spawn(|| {
                    error_span!("node").in_scope(|| node::run(Some(1), true, true))
                });
                let success = match client::run(&path, true, false) {
                    Ok(s) => s,
                    Err(e) => return Err(e),
                };
                broker_thread.join().unwrap()?;
                node_thread.join().unwrap()?;

                if success {
                    println!("✅ All tests passed.")
                } else {
                    println!("❌ Tests failed. See logs.")
                }
            } else if local {
                let broker_thread = thread::spawn(|| broker::run(true));
                let node_thread = thread::spawn(move || node::run(num_threads, true, false));
                client::run(&path, false, !no_tui)?;
                broker_thread.join().unwrap()?;
                node_thread.join().unwrap()?;
            } else {
                client::run(&path, false, !no_tui)?;
            }

            if let Some(buf) = stdout_buffer {
                buf.flush()?;
            }
        }
        Commands::Broker { once } => {
            tracing_subscriber::fmt().with_target(false).init();
            broker::run(once)?;
        }
        Commands::Node { num_threads, local } => {
            tracing_subscriber::fmt().with_target(false).init();
            node::run(num_threads, local, false)?;
        }
        // Commands::Abort => {
        //     let ctx = zmq::Context::new();
        //     let broker_url = env("SQUID_BROKER_URL")? + ":5555";
        //     // Abort cmd is temporary so just blocking and assuming it will connect
        //     let broker_sock = ctx.socket(zmq::DEALER)?;
        //     broker_sock.connect(&broker_url)?;
        //     broker_sock.send("abort", 0)?;
        //     println!("abort signal sent, check other terminal");
        // }
        Commands::Validate { blueprint: bpath } => {
            let blueprint_s = fs::read_to_string(&bpath)
                .with_context(|| format!("Failed to open blueprint file `{}`", bpath.display()))?;
            let blueprint: Blueprint = toml::from_str(&blueprint_s)
                .with_context(|| format!("Failed to parse blueprint file `{}`", bpath.display()))?;
            blueprint.validate()?;
            println!("✅ Blueprint `{}` is valid", bpath.display());
        }
    }

    Ok(())
}
