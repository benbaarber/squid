use core::str;
use std::{
    ops::ControlFlow,
    thread,
    time::{Duration, Instant},
};

use crate::util::{NodeStatus, de_u32, de_u64, docker, env};
use anyhow::Result;
use tracing::{error, info, warn};

pub fn run(threads: Option<usize>, local: bool, test: bool) -> Result<()> {
    assert!(docker::is_installed()?);

    let id: u64 = rand::random();
    let id_b = id.to_be_bytes();
    let id_hex = format!("{:x}", id);

    let mut broker_base_url = env("SQUID_BROKER_URL")?;
    let mg_sock_url = format!("{}:5556", &broker_base_url);
    if broker_base_url == "tcp://localhost" {
        broker_base_url = "tcp://172.17.0.1".to_string();
    }

    let _status = NodeStatus::Idle;
    let cores = thread::available_parallelism()?.get();
    let num_threads = threads.unwrap_or(cores).min(cores);
    let num_threads_s = num_threads.to_string();

    info!("🐋 Squid node starting with ID {}", &id_hex);
    info!("🐋 Detected {} cores", cores);
    info!("🐋 Max worker threads: {}", &num_threads_s);

    let ctx = zmq::Context::new();
    let broker_sock = ctx.socket(zmq::DEALER)?;
    broker_sock.set_identity(&id_b)?;
    broker_sock.connect(&mg_sock_url)?;

    let register = || {
        broker_sock
            .send_multipart(
                [
                    "register".as_bytes(),
                    (num_threads as u32).to_be_bytes().as_slice(),
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
                    info!("🦑 Registered with squid broker at {}", &broker_base_url);
                    break;
                } else {
                    warn!(
                        "Squid broker at {} sent invalid response to registration: `{}`",
                        &broker_base_url,
                        str::from_utf8(&msgb[0]).unwrap(),
                    );
                }
            } else {
                warn!("Squid broker was unavailable at {}", &broker_base_url);
            };

            tries += 1;
            warn!("Retrying... (Attempt {})", tries);
        }
    };

    register();

    const BK_HB_INTERVAL: Duration = Duration::from_secs(1);
    const BK_TTL: Duration = Duration::from_secs(3);
    let mut last_bk_hb_out = Instant::now();
    let mut last_bk_hb_in = Instant::now();

    let mut node_loop = || -> Result<ControlFlow<()>> {
        if last_bk_hb_out.elapsed() > BK_HB_INTERVAL {
            if last_bk_hb_in.elapsed() > BK_TTL {
                warn!("⛓️‍💥 Lost connection to Squid broker. Reconnecting...");
                register();
            }
            broker_sock.send("hb".as_bytes(), 0)?;
            last_bk_hb_out = Instant::now();
        };

        let timeout = BK_HB_INTERVAL
            .checked_sub(last_bk_hb_out.elapsed())
            .map(|x| x.as_millis() as i64)
            .unwrap_or(0);

        if broker_sock.poll(zmq::POLLIN, timeout)? == 0 {
            return Ok(ControlFlow::Continue(()));
        };

        let msgb = broker_sock.recv_multipart(0)?;
        let cmd = &msgb[0][..];
        match cmd {
            b"hb" => {
                last_bk_hb_in = Instant::now();
            }
            b"spawn" => {
                let exp_id_b = msgb[1].as_slice();
                let exp_id = de_u64(&msgb[1])?;
                let exp_id_hex = format!("{:x}", exp_id);
                info!("Spawning container for experiment {}", &exp_id_hex);
                let task_image = str::from_utf8(&msgb[2])?;
                let port = de_u32(&msgb[3])?.to_string();
                // println!("TASK IMAGE: {}", &task_image);
                // println!("TASK PORT: {}", &port);
                // println!("TASK THREADS: {}", &num_threads_s);
                // println!("TASK ID: {}", &exp_id_hex);
                // println!("TASK BROKER URL: {}", &broker_base_url);
                // TODO FINISH
                if !local && !task_image.starts_with("docker.io/library/") {
                    send_status(&broker_sock, exp_id_b, NodeStatus::Pulling)?;
                    if !docker::pull(task_image)?.success() {
                        send_status(&broker_sock, exp_id_b, NodeStatus::Crashed)?;
                        return Ok(ControlFlow::Continue(()));
                    }
                }
                send_status(&broker_sock, exp_id_b, NodeStatus::Active)?;

                if test {
                    docker::test_run(
                        task_image,
                        &exp_id_hex,
                        &broker_base_url,
                        &port,
                        &num_threads_s,
                    )?;
                } else {
                    docker::run(
                        task_image,
                        &exp_id_hex,
                        &broker_base_url,
                        &port,
                        &num_threads_s,
                    )?;
                }

                if local {
                    return Ok(ControlFlow::Break(()));
                }
            }
            b"abort" => {
                let exp_id = de_u64(&msgb[1])?;
                let exp_id_hex = format!("{:x}", exp_id);
                info!("Aborting experiment {}", &exp_id_hex);
                docker::kill_all(&exp_id_hex)?;
            }
            _ => (),
        }

        Ok(ControlFlow::Continue(()))
    };

    loop {
        match node_loop() {
            Ok(ControlFlow::Continue(_)) => continue,
            Ok(ControlFlow::Break(_)) => break,
            Err(e) => {
                // let _ = send_status(&broker_sock, exp, NodeStatus::Crashed);
                error!("Error: {}", &e);
            }
        }
    }

    Ok(())
}

fn send_status(broker_sock: &zmq::Socket, exp_id_b: &[u8], status: NodeStatus) -> Result<()> {
    broker_sock.send_multipart(
        ["status".as_bytes(), exp_id_b, &(status as u8).to_be_bytes()],
        0,
    )?;
    Ok(())
}
