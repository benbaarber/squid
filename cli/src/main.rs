mod tui;

use core::str;
use std::{
    fs::{self, File},
    io::BufReader,
    ops::ControlFlow,
    path::PathBuf,
    thread,
};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use nanoid::nanoid;
use shared::Blueprint;
use tui::handle_progress;

#[derive(Parser)]
#[command(about = "Squid CLI client")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run an experiment
    Run {
        /// Blueprint file
        #[arg(short, long, value_name = "FILE", default_value = "./blueprint.json")]
        blueprint: PathBuf,
    },
    /// Abort the running experiment
    Abort,
    // {
    //     /// Experiment id, printed after calling `squid run`
    //     // id: String,
    // },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let broker_url =
        std::env::var("SQUID_BROKER_DEALER_URL").context("$SQUID_BROKER_DEALER_URL not set")?;

    let ctx = zmq::Context::new();

    match &cli.command {
        Commands::Run { blueprint: bpath } => {
            let file = File::open(bpath)
                .with_context(|| format!("Failed to open blueprint file {:?}", bpath))?;
            println!("🔧 Read blueprint from {:?}", bpath.canonicalize()?);
            let reader = BufReader::new(file);
            let _blueprint: Blueprint = serde_json::from_reader(reader)?;
            let blueprint_b = fs::read(bpath).expect("Blueprint file already verified");

            // let mut tracker = Tracker::new(&blueprint);

            let b_sock = ctx.socket(zmq::PAIR)?;
            b_sock.bind("inproc://broker_loop")?;

            let b_thread = thread::spawn(move || -> Result<String> {
                let cli_sock = ctx.socket(zmq::PAIR)?;
                cli_sock.connect("inproc://broker_loop")?;

                let broker_sock = ctx.socket(zmq::DEALER)?;
                broker_sock.set_linger(0)?;
                broker_sock.connect(&broker_url)?;

                broker_sock.send("ping", 0)?;
                if broker_sock.poll(zmq::POLLIN, 5000)? > 0 {
                    if broker_sock.recv_bytes(0)?.starts_with(b"pong") {
                        println!("🔗 Connected to Squid broker at {}", &broker_url);
                    }
                } else {
                    bail!("Squid broker was unresponsive at {}", &broker_url);
                }

                let id = nanoid!(8);
                println!("🧪 Starting experiment {}", &id);

                broker_sock.send_multipart([b"run", id.as_bytes(), blueprint_b.as_slice()], 0)?;

                loop {
                    match broker_loop(&broker_sock, &cli_sock) {
                        Ok(ControlFlow::Break(result)) => break Ok(result),
                        Ok(ControlFlow::Continue(_)) => continue,
                        Err(e) => {
                            eprintln!("Error: {}", e)
                        }
                    }
                }
            });

            let result = loop {
                if b_thread.is_finished() {
                    break b_thread.join().unwrap()?;
                }

                if b_sock.poll(zmq::POLLIN, 100)? > 0 {
                    let msgb = b_sock.recv_multipart(0)?;
                    handle_progress(&msgb)?;
                }
            };

            println!("Done, received: {}", result);
        }
        Commands::Abort => {
            // Abort cmd is temporary so just blocking and assuming it will connect
            let broker_sock = ctx.socket(zmq::DEALER)?;
            broker_sock.connect(&broker_url)?;
            broker_sock.send("abort", 0)?;
            println!("abort signal sent, check other terminal");
        }
    }

    Ok(())
}

fn broker_loop(broker_sock: &zmq::Socket, cli_sock: &zmq::Socket) -> Result<ControlFlow<String>> {
    let msgb = broker_sock.recv_multipart(0)?;

    let cmd = msgb[0].as_slice();
    let _id = str::from_utf8(&msgb[1])?;
    match cmd {
        b"progress" => {
            cli_sock.send_multipart(&msgb[2..], 0)?;
        }
        b"done" => {
            let result = String::from_utf8(msgb[2].clone())?;
            return Ok(ControlFlow::Break(result));
        }
        b"error" => {
            let msg = str::from_utf8(&msgb[2])?;
            eprintln!("Error: {}", msg);
            // TODO: determine if fatal and recover?
        }
        x => bail!("Error: {}", str::from_utf8(x)?),
    }

    Ok(ControlFlow::Continue(()))
}
