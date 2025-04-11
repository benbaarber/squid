mod broker;
mod client;
mod node;
mod util;

use core::str;
use std::{
    fs::{self},
    path::PathBuf,
};

use crate::util::{Blueprint, env};
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(about = "Squid - distributed neuroevolution")]
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
        // /// Run in test mode (spawns a single worker container locally and simulates a single agent)
        // #[arg(short, long)]
        // test: bool,
    },
    /// Run the squid broker
    Broker,
    /// Run the squid node
    Node {
        /// Number of worker threads to spawn, defaults to one container per core
        #[arg(short, long)]
        threads: Option<usize>,
        /// For testing only. Run in local mode, assume any task image passed is already in the local docker library.
        #[arg(short, long)]
        local: bool,
    },
    /// Validate a blueprint file
    Validate {
        /// Blueprint file
        #[arg(short, long, value_name = "FILE", default_value = "./squid.toml")]
        blueprint: PathBuf,
    },
    /// Abort the running experiment (deprecated)
    Abort,
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
        Commands::Run { path } => {
            client::run(&path)?;
        }
        Commands::Broker => {
            broker::run()?;
        }
        Commands::Node { threads, local } => {
            node::run(threads, local)?;
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
            let blueprint_s = fs::read_to_string(&bpath)
                .with_context(|| format!("Failed to open blueprint file `{}`", bpath.display()))?;
            let blueprint: Blueprint = toml::from_str(&blueprint_s)
                .with_context(|| format!("Failed to parse blueprint file `{}`", bpath.display()))?;
            blueprint.validate()?;
            println!("✅ Validated blueprint from `{}`", bpath.display());
        }
    }

    Ok(())
}
