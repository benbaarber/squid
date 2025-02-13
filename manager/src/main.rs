use core::str;
use std::{
    collections::HashMap,
    thread,
    time::{Duration, Instant},
};

use anyhow::Result;
use clap::Parser;
use shared::{de_u64, docker, env, to_router_id_array, ManagerStatus};

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

struct Worker {
    exp_id: u64,
    last_pulse: Instant,
}

type Workers = HashMap<[u8; 5], Worker>;

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
    let mut workers = Workers::with_capacity(num_containers);

    println!("🐋 Manager starting up...");
    println!("🐋 Detected {} CPUs", cpus);
    println!("🐋 Max containers: {}", num_containers);

    let ctx = zmq::Context::new();
    let broker_sock = ctx.socket(zmq::DEALER)?;
    broker_sock.connect(&mg_sock_url)?;
    let worker_sock = ctx.socket(zmq::ROUTER)?;
    worker_sock.bind("tcp://172.17.0.1:5554")?;

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

    const WK_HB_INTERVAL: Duration = Duration::from_secs(1);
    const WK_TTL: Duration = Duration::from_secs(3);
    const BK_HB_INTERVAL: Duration = Duration::from_secs(5);
    const BK_TTL: Duration = Duration::from_secs(15);
    let mut last_wk_hb_out = Instant::now();
    let mut last_bk_hb_out = Instant::now();
    let mut last_bk_hb_in = Instant::now();

    let mut sockets = [
        broker_sock.as_poll_item(zmq::POLLIN),
        worker_sock.as_poll_item(zmq::POLLIN),
    ];

    let mut manager_loop = || -> Result<()> {
        if last_bk_hb_out.elapsed() > BK_HB_INTERVAL {
            if last_bk_hb_in.elapsed() > BK_TTL {
                println!("⛓️‍💥 Lost connection to Squid broker. Reconnecting...");
                register();
            }
            broker_sock.send("hb".as_bytes(), 0)?;
            last_bk_hb_out = Instant::now();
        };

        if workers.len() > 0 && last_wk_hb_out.elapsed() > WK_HB_INTERVAL {
            workers.retain(|id, wk| {
                if wk.last_pulse.elapsed() > WK_TTL {
                    return false;
                }
                worker_sock.send_multipart([&id[..], b"hb"], 0).unwrap();
                true
            });
            last_wk_hb_out = Instant::now();
        }

        let timeout = if workers.len() > 0 {
            WK_HB_INTERVAL
                .checked_sub(last_wk_hb_out.elapsed())
                .map(|x| x.as_millis() as i64)
                .unwrap_or(0)
        } else {
            BK_HB_INTERVAL
                .checked_sub(last_bk_hb_out.elapsed())
                .map(|x| x.as_millis() as i64)
                .unwrap_or(0)
        };

        if zmq::poll(&mut sockets, timeout)? == 0 {
            return Ok(());
        }

        if sockets[0].is_readable() {
            let msgb = broker_sock.recv_multipart(0)?;
            let cmd = msgb[0].as_slice();
            match cmd {
                b"hb" => {
                    last_bk_hb_in = Instant::now();
                }
                b"spawn" => {
                    let id = de_u64(&msgb[1])?;
                    let id_hex = format!("{:x}", id);
                    println!("SPAWN ID: {}", &id_hex);
                    let task_image = str::from_utf8(&msgb[2])?;
                    if !args.local {
                        send_status(&broker_sock, ManagerStatus::Pulling)?;
                        if !docker::pull(task_image)?.success() {
                            send_status(&broker_sock, ManagerStatus::Crashed)?;
                            return Ok(());
                        }
                    }
                    send_status(&broker_sock, ManagerStatus::Active)?;

                    let label = format!("squid_id={}", &id_hex);
                    for _ in 0..num_containers {
                        docker::run(task_image, &broker_wk_env, &label, &id_hex)?;
                    }
                }
                b"abort" => {
                    let id = de_u64(&msgb[1])?;
                    let id_hex = format!("{:x}", id);
                    println!("ABORT ID: {}", &id_hex);
                    docker::kill_all(&id_hex)?;
                }
                _ => (),
            }
        }

        while let Ok(msgb) = worker_sock.recv_multipart(zmq::DONTWAIT) {
            let wkid = to_router_id_array(msgb[0].clone())?;
            let cmd = &msgb[1][..];
            match cmd {
                b"hb" => {
                    workers
                        .entry(wkid)
                        .and_modify(|wk| wk.last_pulse = Instant::now());
                }
                b"register" => {
                    let exp_id = de_u64(&msgb[2])?;
                    workers.insert(
                        wkid,
                        Worker {
                            exp_id,
                            last_pulse: Instant::now(),
                        },
                    );
                }
                b"drop" => {
                    workers.remove(&wkid);
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
