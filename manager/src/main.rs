use core::str;
use std::{
    thread,
    time::{Duration, Instant},
};

use anyhow::Result;
use clap::Parser;
use shared::{docker, env, ManagerStatus};

#[derive(Parser)]
#[command(about = "Squid Manager")]
struct Args {
    /// Number of containers to spawn, defaults to one container per core
    #[arg(short, long)]
    containers: Option<usize>,
    /// For testing only. Run in local mode, assume any task image passed is already in the local docker library.
    #[arg(short, long)]
    local: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    assert!(docker::is_installed()?);

    let broker_url = env("SQUID_BROKER_URL")?;
    let mg_sock_url = format!("{}:5556", &broker_url);
    let broker_wk_env = if broker_url != "tcp://localhost" {
        format!("SQUID_BROKER_WK_SOCK_URL={}:5557", &broker_url)
    } else {
        "SQUID_BROKER_WK_SOCK_URL=tcp://172.17.0.1:5557".to_string()
    };

    let _status = ManagerStatus::Idle;
    let cpus = thread::available_parallelism()?.get();
    let num_containers = args.containers.unwrap_or(cpus).min(cpus);

    println!("🐋 Manager starting up...");
    println!("🐋 Detected {} CPUs", cpus);
    println!("🐋 Max containers: {}", num_containers);

    let ctx = zmq::Context::new();
    let broker_sock = ctx.socket(zmq::DEALER)?;
    broker_sock.connect(&mg_sock_url)?;

    let register = || {
        broker_sock
            .send_multipart(
                [
                    "register".as_bytes(),
                    &(num_containers as u32).to_le_bytes(),
                ],
                0,
            )
            .unwrap();

        let mut tries = 0;
        loop {
            let timeout = 5000 * (tries + 1).min(6);
            if broker_sock.poll(zmq::POLLIN, timeout).unwrap() > 0 {
                let msgb = broker_sock.recv_multipart(0).unwrap();
                if msgb[0].starts_with(b"registered") {
                    println!("🔗 Registered with squid broker at {}", &broker_url);
                    break;
                } else {
                    println!(
                        "Squid broker at {} sent invalid response to registration: `{}`",
                        &broker_url,
                        str::from_utf8(&msgb[0]).unwrap(),
                    );
                }
            } else {
                println!("Squid broker was unavailable at {}", &broker_url);
            };

            tries += 1;
            println!("Retrying... (Attempt {})", tries);
        }
    };

    register();

    const HB_INTERVAL: Duration = Duration::from_secs(5);
    const BK_TTL: Duration = Duration::from_secs(15);
    let mut last_heartbeat = Instant::now();
    let mut last_broker_pulse = Instant::now();

    let mut manager_loop = || -> Result<()> {
        if last_heartbeat.elapsed() > HB_INTERVAL {
            if last_broker_pulse.elapsed() > BK_TTL {
                println!("⛓️‍💥 Lost connection to Squid broker. Reconnecting...");
                register();
            }
            broker_sock.send("hb".as_bytes(), 0)?;
            last_heartbeat = Instant::now();
        };

        let timeout = HB_INTERVAL
            .checked_sub(last_heartbeat.elapsed())
            .map(|x| x.as_millis() as i64)
            .unwrap_or(0)
            .max(1);

        if broker_sock.poll(zmq::POLLIN, timeout)? > 0 {
            let msgb = broker_sock.recv_multipart(0)?;
            let cmd = msgb[0].as_slice();
            match cmd {
                b"hb" => {
                    last_broker_pulse = Instant::now();
                }
                b"spawn" => {
                    let id = str::from_utf8(&msgb[1])?;
                    println!("SPAWN ID: {}", id);
                    let task_image = str::from_utf8(&msgb[2])?;
                    if !args.local {
                        send_status(&broker_sock, ManagerStatus::Pulling)?;
                        if !docker::pull(task_image)?.success() {
                            send_status(&broker_sock, ManagerStatus::Crashed)?;
                            return Ok(());
                        }
                    }
                    send_status(&broker_sock, ManagerStatus::Active)?;

                    let label = format!("squid_id={}", id);
                    for _ in 0..num_containers {
                        docker::run(task_image, &broker_wk_env, &label, id)?;
                    }
                }
                b"abort" => {
                    let id = str::from_utf8(&msgb[1])?;
                    println!("ABORT ID: {}", id);
                    docker::kill_all(id)?;
                }
                _ => (),
            }
        }
        Ok(())
    };

    loop {
        if let Err(e) = manager_loop() {
            let _ = send_status(&broker_sock, ManagerStatus::Crashed);
            eprintln!("Error: {}", &e);
        }
    }
}

fn send_status(broker_sock: &zmq::Socket, status: ManagerStatus) -> Result<()> {
    broker_sock.send_multipart(["status".as_bytes(), &bincode::serialize(&status)?], 0)?;
    Ok(())
}
