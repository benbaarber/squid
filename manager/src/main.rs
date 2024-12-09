use core::str;
use std::process::Command;

use anyhow::{Context, Result};
use clap::Parser;

#[derive(Parser)]
#[command(about = "Squid Manager")]
struct Args {
    /// Number of containers to spawn, defaults to one container per core
    #[arg(short, long)]
    containers: Option<usize>,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let broker_url =
        std::env::var("SQUID_BROKER_PUB_URL").context("$SQUID_BROKER_PUB_URL not set")?;

    let cpus = num_cpus::get();
    let num_containers = args.containers.unwrap_or(cpus);

    println!("🦑 Manager starting up...");
    println!("🦑 Detected {} CPUs", cpus);
    println!("🦑 Max containers: {}", num_containers);

    let ctx = zmq::Context::new();
    let broker_sub = ctx.socket(zmq::SUB)?;
    broker_sub.set_subscribe(b"spawn")?;
    broker_sub.set_subscribe(b"abort")?;
    broker_sub.connect(&broker_url)?;

    loop {
        let res = manager_loop(&broker_sub, num_containers);
        if let Err(e) = res {
            eprintln!("Error: {}", &e);
        }
    }
}

fn manager_loop(broker_sub: &zmq::Socket, num_containers: usize) -> Result<()> {
    let msgb = broker_sub.recv_multipart(0)?;
    let cmd = msgb[0].as_slice();
    match cmd {
        b"spawn" => {
            let id = str::from_utf8(&msgb[1])?;
            println!("SPAWN ID: {}", id);
            let label = format!("squid_id={}", id);
            let task_image = str::from_utf8(&msgb[2])?;
            for _ in 0..num_containers {
                Command::new("docker")
                    .args(["run", "--rm", "-d", "-l", &label, task_image, id])
                    .spawn()?;
            }
        }
        b"abort" => {
            let id = str::from_utf8(&msgb[1])?;
            println!("ABORT ID: {}", id);
            let label_query = format!("label=squid_id={}", id);

            let output = Command::new("docker")
                .args(["ps", "-q", "-f", &label_query])
                .output()?;
            let container_ids = String::from_utf8_lossy(&output.stdout);

            if container_ids.trim().len() > 0 {
                Command::new("docker")
                    .arg("kill")
                    .args(container_ids.split_whitespace())
                    .spawn()?;
            }
        }
        _ => (),
    }

    Ok(())
}
