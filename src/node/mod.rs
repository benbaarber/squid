use core::str;
use std::{
    collections::HashMap,
    ops::ControlFlow,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::util::{NodeStatus, broadcast_router, de_u32, de_u64, docker};
use anyhow::{Result, bail};
use tracing::{debug, error, info, warn};

pub fn run(broker_addr: String, threads: Option<usize>, local: bool, test: bool) -> Result<()> {
    assert!(docker::is_installed()?);

    let id: u64 = rand::random();
    let id_b = id.to_be_bytes();
    let id_hex = format!("{:x}", id);

    let broker_url = format!("tcp://{}:5556", &broker_addr);

    let _status = NodeStatus::Idle;
    let cores = thread::available_parallelism()?.get();
    let num_threads = threads
        .unwrap_or_else(|| if test { 1 } else { cores })
        .min(cores);
    let num_threads_s = num_threads.to_string();

    info!(
        "🐋 Squid node v{} starting with ID {}",
        env!("CARGO_PKG_VERSION"),
        &id_hex
    );
    info!("🐋 Detected {} cores", cores);
    info!("🐋 Max workers: {}", &num_threads_s);

    let mut dock_threads = HashMap::<u64, JoinHandle<()>>::new();

    let ctx = zmq::Context::new();
    let broker_sock = ctx.socket(zmq::DEALER)?;
    broker_sock.set_identity(&id_b)?;
    broker_sock.connect(&broker_url)?;

    let dock_sock = ctx.socket(zmq::ROUTER)?;
    dock_sock.bind("inproc://docker")?;

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
                    info!("🦑 Registered with squid broker at {}", &broker_url);
                    break;
                } else {
                    warn!(
                        "Squid broker at {} sent invalid response to registration: `{}`",
                        &broker_url,
                        str::from_utf8(&msgb[0]).unwrap(),
                    );
                }
            } else {
                warn!("Squid broker was unavailable at {}", &broker_url);
            };

            tries += 1;
            warn!("Retrying... (Attempt {})", tries);
        }
    };

    register();

    const BK_HB_INTERVAL: Duration = Duration::from_secs(1);
    const BK_TTL: Duration = Duration::from_secs(5);
    let mut last_bk_hb_out = Instant::now();
    let mut last_bk_hb_in = Instant::now();

    let mut sockets = [
        broker_sock.as_poll_item(zmq::POLLIN),
        dock_sock.as_poll_item(zmq::POLLIN),
    ];

    let mut node_loop = || -> Result<ControlFlow<()>> {
        if last_bk_hb_out.elapsed() > BK_HB_INTERVAL {
            if last_bk_hb_in.elapsed() > BK_TTL {
                warn!("⛓️‍💥 Lost connection to Squid broker. Reconnecting...");
                register();
            }
            broker_sock.send("hb".as_bytes(), 0)?;
            last_bk_hb_out = Instant::now();

            dock_threads.retain(|_, t| !t.is_finished());
        };

        let timeout = BK_HB_INTERVAL
            .checked_sub(last_bk_hb_out.elapsed())
            .map(|x| x.as_millis() as i64)
            .unwrap_or(0);

        if zmq::poll(&mut sockets, timeout)? == 0 {
            return Ok(ControlFlow::Continue(()));
        }

        while let Ok(msgb) = broker_sock.recv_multipart(zmq::DONTWAIT) {
            let cmd = msgb[0].as_slice();
            match cmd {
                b"hb" => {
                    last_bk_hb_in = Instant::now();
                }
                b"spawn" => {
                    let exp_id_b = msgb[1].clone();

                    let exp_id = de_u64(&msgb[1])?;
                    let exp_id_x = format!("{:x}", exp_id);
                    let task_image = String::from_utf8(msgb[2].clone())?;
                    let port = de_u32(&msgb[3])?; // first port of the group of 3

                    info!("Spawning container for experiment {}", &exp_id_x);
                    debug!("TASK IMAGE: {}", &task_image);
                    debug!("TASK PORT: {}", port);
                    debug!("TASK THREADS: {}", &num_threads_s);
                    debug!("TASK BROKER ADDR: {}", &broker_addr);

                    let ctx = ctx.clone();
                    let broker_addr = broker_addr.clone();
                    let num_threads_s = num_threads_s.clone();
                    let handle = thread::spawn(move || {
                        let main_sock = ctx.socket(zmq::DEALER).unwrap();
                        main_sock.set_identity(&exp_id_b).unwrap();
                        main_sock.connect("inproc://docker").unwrap();

                        let send_status = |status| {
                            debug!("Status: {:?}", status);
                            main_sock.send_multipart(["status".as_bytes(), &[status as u8]], 0)
                        };

                        let spawn_ct = || {
                            if !local && !task_image.starts_with("docker.io/library/") {
                                send_status(NodeStatus::Pulling)?;
                                let mut task = docker::pull(&task_image)?;

                                // Allow client to abort during pull
                                while task.try_wait()?.is_none() {
                                    if main_sock.poll(zmq::POLLIN, 500)? > 0 {
                                        let msg = main_sock.recv_msg(0)?;
                                        if &msg[..] == b"abort" {
                                            debug!("Aborting docker pull task");
                                            task.kill()?;
                                            return Ok(());
                                        }
                                    }
                                }

                                if !task.wait()?.success() {
                                    bail!("Failed to pull docker image");
                                }
                            }

                            let wk_broker_addr = if broker_addr == "localhost" {
                                // Set to docker internal ip for localhost
                                "172.17.0.1".to_string()
                            } else {
                                broker_addr
                            };

                            // Spawn worker containers
                            let exp_id_label = "squid_exp_id=".to_string() + &exp_id_x;
                            let exp_id_env = "SQUID_EXP_ID=".to_string() + &exp_id_x;
                            let addr_env = "SQUID_ADDR=".to_string() + &wk_broker_addr;
                            let port_env = "SQUID_PORT=".to_string() + &port.to_string();
                            let num_threads_env = "SQUID_NUM_THREADS=".to_string() + &num_threads_s;
                            if test {
                                docker::test_run(
                                    &task_image,
                                    &exp_id_label,
                                    &exp_id_env,
                                    &addr_env,
                                    &port_env,
                                    &num_threads_env,
                                )?;
                            } else {
                                docker::run(
                                    &task_image,
                                    &exp_id_label,
                                    &exp_id_env,
                                    &addr_env,
                                    &port_env,
                                    &num_threads_env,
                                )?;
                            }

                            send_status(NodeStatus::Active)?;
                            Ok(())
                        };

                        if let Err(e) = spawn_ct() {
                            send_status(NodeStatus::Crashed).unwrap();
                            error!("{:?}", e);
                        }
                    });

                    dock_threads.insert(exp_id, handle);

                    // Supervisor thread
                    // TODO move this to function to catch errors, kill workers on disconnect
                    // let ctx_clone = ctx.clone();
                    // let exp_id_x_trunc = format!("{:x}", exp_id >> 32);
                    // let supervisor_span = error_span!("supervisor", exp_id = &exp_id_x_trunc);
                    // let supervisor_thread = thread::spawn(move || -> Result<()> {
                    //     let _guard = supervisor_span.enter();
                    //     supervisor(
                    //         exp_id,
                    //         id,
                    //         ctx_clone,
                    //         task_image,
                    //         wk_broker_addr,
                    //         port,
                    //         num_threads,
                    //         test,
                    //     )
                    // });
                }
                b"abort" => {
                    let exp_id_b = msgb[1].as_slice();
                    let exp_id = de_u64(exp_id_b)?;
                    let exp_id_hex = format!("{:x}", exp_id);

                    info!("Aborting experiment {}", &exp_id_hex);

                    dock_sock.send_multipart([exp_id_b, b"abort"], 0)?;
                    dock_threads.remove(&exp_id);

                    let exp_id_label = format!("label=squid_exp_id={}", &exp_id_hex);
                    docker::kill_by_exp(&exp_id_label)?;
                }
                b"kill" => {
                    broadcast_router(&dock_threads, &dock_sock, &["abort".as_bytes()])?;
                    docker::kill_by_exp("label=squid_exp_id")?;
                    return Ok(ControlFlow::Break(()));
                }
                _ => (),
            }
        }

        while let Ok(msgb) = dock_sock.recv_multipart(zmq::DONTWAIT) {
            let exp_id_b = msgb[0].as_slice();
            let cmd = msgb[1].as_slice();
            match cmd {
                b"status" => {
                    let status_b = msgb[2].as_slice();
                    broker_sock.send_multipart(["status".as_bytes(), exp_id_b, status_b], 0)?;
                }
                _ => (),
            }
        }

        Ok(ControlFlow::Continue(()))
    };

    loop {
        match node_loop() {
            Ok(ControlFlow::Continue(_)) => continue,
            Ok(ControlFlow::Break(_)) => break,
            Err(e) => {
                error!("Error: {}", &e);
            }
        }
    }

    Ok(())
}

// Supervisor currently operates in the worker
// fn supervisor(
//     exp_id: u64,
//     node_id: u64,
//     ctx: zmq::Context,
//     task_image: String,
//     broker_addr: String,
//     port: u32,
//     num_workers: usize,
//     test: bool,
// ) -> Result<()> {
//     let exp_id_b = exp_id.to_be_bytes();
//     let exp_id_x = format!("{:x}", exp_id);
//     let node_id_b = node_id.to_be_bytes();
//
//     let sv_url = format!("tcp://{}:{}", &broker_addr, port + 2);
//     let ga_sock = ctx.socket(zmq::DEALER)?;
//     // use node id as supervisor id
//     ga_sock.set_identity(&node_id_b)?;
//     ga_sock.set_linger(0)?;
//     ga_sock.connect(&sv_url)?;
//
//     let worker_ids = (0..num_workers)
//         .map(|_| rand::random::<u64>())
//         .collect::<Vec<_>>();
//     let worker_ids_b = serde_json::to_vec(&worker_ids)?;
//     let mut worker_ids_x = worker_ids
//         .iter()
//         .map(|x| format!("{:x}", x))
//         .collect::<Vec<_>>();
//     ga_sock.send_multipart([exp_id_b.as_slice(), b"register", &worker_ids_b], 0)?;
//
//     if ga_sock.poll(zmq::POLLIN, 5000)? > 0 {
//         let msgb = ga_sock.recv_multipart(0)?;
//         let cmd = msgb[0].as_slice();
//         match cmd {
//             b"registered" => {
//                 info!("Registered with experiment thread");
//             }
//             b"stop" | b"kill" => {
//                 return Ok(());
//             }
//             x => error!(
//                 "Broker sent invalid response to registration: {}",
//                 str::from_utf8(x)?
//             ),
//         }
//     } else {
//         error!("Broker did not respond. Terminating.");
//         return Ok(());
//     }
//
//     // Spawn worker containers
//     let exp_id_label = "squid_exp_id=".to_string() + &exp_id_x;
//     let exp_id_label_query = "label=".to_string() + &exp_id_label;
//     let exp_id_env = "SQUID_EXP_ID=".to_string() + &exp_id_x;
//     let addr_env = "SQUID_ADDR=".to_string() + &broker_addr;
//     let port_env = "SQUID_PORT=".to_string() + &port.to_string();
//     for wk_id_x in &worker_ids_x {
//         let wk_id_label = format!("squid_worker_id={}", wk_id_x);
//         let wk_id_env = format!("SQUID_WORKER_ID={}", wk_id_x);
//         if test {
//             docker::test_run(
//                 &task_image,
//                 &exp_id_label,
//                 &wk_id_label,
//                 &exp_id_env,
//                 &addr_env,
//                 &port_env,
//                 &wk_id_env,
//             )?;
//         } else {
//             docker::run(
//                 &task_image,
//                 &exp_id_label,
//                 &wk_id_label,
//                 &exp_id_env,
//                 &addr_env,
//                 &port_env,
//                 &wk_id_env,
//             )?;
//         }
//     }
//
//     const BK_TTL_MS: i64 = 15000;
//     let mut supervisor_loop = || -> Result<ControlFlow<()>> {
//         if ga_sock.poll(zmq::POLLIN, BK_TTL_MS)? > 0 {
//             let msgb = ga_sock.recv_multipart(0)?;
//             let cmd = msgb[0].as_slice();
//             match cmd {
//                 b"hb" => {
//                     let alive_worker_ids = docker::ps_by_exp(&exp_id_label_query)?;
//                     let mut dead = Vec::with_capacity(worker_ids_x.len());
//                     worker_ids_x.retain(|id| {
//                         if !alive_worker_ids.contains(id) {
//                             dead.push(
//                                 u64::from_str_radix(id, 16).expect("Worker id is hex string"),
//                             );
//                             false
//                         } else {
//                             true
//                         }
//                     });
//                     if dead.len() > 0 {
//                         let dead_b = serde_json::to_vec(&dead)?;
//                         ga_sock.send_multipart([exp_id_b.as_slice(), b"dead", &dead_b], 0)?;
//                     } else {
//                         ga_sock.send_multipart([exp_id_b.as_slice(), b"ok"], 0)?;
//                     }
//                 }
//                 b"registered" => (),
//                 b"stop" => return Ok(ControlFlow::Break(())),
//                 b"kill" => {
//                     info!("Experiment thread sent kill signal. Terminating.");
//                     docker::kill_by_exp(&exp_id_label_query)?;
//                     return Ok(ControlFlow::Break(()));
//                 }
//                 _ => (),
//             }
//         } else {
//             error!("Lost connection to broker experiment thread. Terminating.");
//             docker::kill_by_exp(&exp_id_label_query)?;
//             return Ok(ControlFlow::Break(()));
//         }
//
//         Ok(ControlFlow::Continue(()))
//     };
//
//     loop {
//         match supervisor_loop() {
//             Ok(ControlFlow::Continue(_)) => continue,
//             Ok(ControlFlow::Break(_)) => break,
//             Err(e) => {
//                 error!("Supervisor loop: {:?}", e);
//                 ga_sock
//                     .send_multipart([exp_id_b.as_slice(), b"error", e.to_string().as_bytes()], 0)?;
//             }
//         }
//     }
//
//     Ok(())
// }
//
