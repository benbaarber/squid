use core::str;
use std::process::Command;

use anyhow::{bail, Context, Result};

fn main() -> Result<()> {
    let broker_url =
        std::env::var("SQUID_BROKER_PUB_URL").context("$SQUID_BROKER_PUB_URL not set")?;

    let ctx = zmq::Context::new();
    let broker_sub = ctx.socket(zmq::SUB)?;
    broker_sub.set_subscribe(b"spawn")?;
    broker_sub.set_subscribe(b"abort")?;
    broker_sub.connect(&broker_url)?;

    loop {
        let res = worker_loop(&broker_sub);
        if let Err(e) = res {
            eprintln!("Error: {}", &e);
        }
    }
}

fn worker_loop(broker_sub: &zmq::Socket) -> Result<()> {
    let msgb = broker_sub.recv_multipart(0)?;
    let cmd = msgb[0].as_slice();
    match cmd {
        b"spawn" => {
            let id = str::from_utf8(&msgb[1])?;
            println!("SPAWN ID: {}", id);
            let label = format!("squid_id={}", id);
            let task_image = str::from_utf8(&msgb[2])?;
            let cpus = num_cpus::get_physical();
            for i in 0..cpus {
                Command::new("docker")
                    .args([
                        "run",
                        "--rm",
                        "-d",
                        "-l",
                        &label,
                        format!("--cpuset-cpus={}", i).as_str(),
                        task_image,
                    ])
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
        x => {
            bail!("Received invalid command: {}", str::from_utf8(x)?);
        }
    }

    Ok(())
}
