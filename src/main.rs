mod broker;
mod client;
mod node;
mod util;

use core::str;
use std::{fs, path::PathBuf, thread};

use crate::util::blueprint::Blueprint;
use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use tracing::{error_span, level_filters::LevelFilter};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use util::{env, stdout_buffer::StdoutBuffer};

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
        /// Broker address (e.g. 192.168.0.42)
        #[arg(short, long)]
        broker_addr: Option<String>,
        /// Run in local mode - spawns a squid broker and node locally and runs the experiment on your device
        #[arg(short, long)]
        local: bool,
        /// If running in local or test mode, specifies number of worker threads to spawn (local default: use all available cores, test default: 1)
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
        /// Broker address (e.g. 192.168.0.42)
        #[arg(short, long)]
        broker_addr: Option<String>,
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
}

fn main() -> Result<()> {
    let args = Args::parse();

    let log_level = match std::env::var("SQUID_LOG")
        .unwrap_or_else(|_| "INFO".to_string())
        .to_uppercase()
        .as_str()
    {
        "OFF" => LevelFilter::OFF,
        "ERROR" => LevelFilter::ERROR,
        "WARN" => LevelFilter::WARN,
        "INFO" => LevelFilter::INFO,
        "DEBUG" => LevelFilter::DEBUG,
        "TRACE" => LevelFilter::TRACE,
        x => {
            eprintln!("Invalid log level: {}", x);
            eprintln!("Using default log level: INFO");
            LevelFilter::INFO
        }
    };

    match args.command {
        Commands::Init { path } => {
            fs::create_dir_all(&path)?;
            fs::write(
                path.join("squid.toml"),
                include_bytes!("../templates/squid.toml"),
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
            broker_addr,
            local,
            num_threads,
            test,
            no_tui,
        } => {
            bail_assert!(path.exists(), "No such file or directory: {:?}", &path);

            if num_threads.is_some_and(|t| t == 0) {
                bail!("`num_threads` must be greater than 0");
            }

            // Setup logging
            let mut stdout_buffer = None;
            let filter =
                tracing_subscriber::filter::Targets::new().with_target("squid::client", log_level);
            if test {
                tracing_subscriber::fmt()
                    .with_max_level(log_level)
                    .with_target(false)
                    .init();
            } else if no_tui {
                tracing_subscriber::registry()
                    .with(tracing_subscriber::fmt::layer().with_target(false))
                    .with(filter)
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
                    .with(filter)
                    .init();
                tui_logger::init_logger(tui_logger::LevelFilter::Info).unwrap();
            }

            let broker_addr = if test || local {
                "localhost".to_string()
            } else {
                match broker_addr {
                    Some(x) => x,
                    None => env("SQUID_BROKER_ADDR").map_err(|_| anyhow::Error::msg("Broker address not set. Use cli argument `-b`/`--broker-addr` or set $SQUID_BROKER_ADDR env variable."))?
                }
            };

            if test {
                let broker_thread =
                    thread::spawn(|| error_span!("broker").in_scope(|| broker::run(true)));
                let broker_url_clone = broker_addr.clone();
                let node_thread = thread::spawn(move || {
                    error_span!("node")
                        .in_scope(|| node::run(broker_url_clone, num_threads, true, true))
                });
                let success =
                    client::run(&path, broker_addr, Some(num_threads.unwrap_or(1)), false)?;
                broker_thread.join().unwrap()?;
                node_thread.join().unwrap()?;

                if success {
                    println!("✅ All tests passed.")
                } else {
                    println!("❌ Tests failed. See logs.")
                }
            } else if local {
                let broker_thread = thread::spawn(|| broker::run(true));
                let broker_url_clone = broker_addr.clone();
                let node_thread =
                    thread::spawn(move || node::run(broker_url_clone, num_threads, true, false));
                client::run(&path, broker_addr, None, !no_tui)?;
                broker_thread.join().unwrap()?;
                node_thread.join().unwrap()?;
            } else {
                client::run(&path, broker_addr, None, !no_tui)?;
            }

            if let Some(buf) = stdout_buffer {
                buf.flush()?;
            }
        }
        Commands::Broker { once } => {
            tracing_subscriber::fmt()
                .with_target(false)
                .with_max_level(log_level)
                .init();
            broker::run(once)?;
        }
        Commands::Node {
            broker_addr,
            num_threads,
            local,
        } => {
            tracing_subscriber::fmt()
                .with_target(false)
                .with_max_level(log_level)
                .init();
            let broker_addr = if local {
                "localhost".to_string()
            } else {
                match broker_addr {
                    Some(x) => x,
                    None => env("SQUID_BROKER_ADDR").map_err(|_| anyhow::Error::msg("Broker address not set. Use cli argument `-b`/`--broker-addr` or set $SQUID_BROKER_ADDR env variable."))?
                }
            };
            node::run(broker_addr, num_threads, local, false)?;
        }
        Commands::Validate { blueprint: bpath } => {
            let blueprint_s = fs::read_to_string(&bpath)
                .with_context(|| format!("Failed to open blueprint file `{}`", bpath.display()))?;
            let mut blueprint: Blueprint = toml::from_str(&blueprint_s)
                .with_context(|| format!("Failed to parse blueprint file `{}`", bpath.display()))?;
            blueprint.validate()?;
            println!("✅ Blueprint `{}` is valid", bpath.display());
        }
    }

    Ok(())
}
