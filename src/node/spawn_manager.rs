use std::sync::Arc;

use super::node::NodeMeta;
use crate::{NodeStatus, docker};
use anyhow::{Result, bail};
use tracing::{debug, error};

pub struct SpawnManager {
    meta: Arc<NodeMeta>,
    exp_id: u64,
    task_image: String,
    port: u32,
    nd_dealer: zmq::Socket,
}

impl SpawnManager {
    pub fn new(
        meta: Arc<NodeMeta>,
        ctx: zmq::Context,
        exp_id: u64,
        task_image: String,
        port: u32,
    ) -> Result<Self> {
        let nd_dealer = ctx.socket(zmq::DEALER)?;
        nd_dealer.set_identity(&exp_id.to_be_bytes())?;
        nd_dealer.connect("inproc://docker")?;

        let dm = SpawnManager {
            meta,
            exp_id,
            task_image,
            port,
            nd_dealer,
        };

        Ok(dm)
    }

    pub fn run(&mut self) {
        if let Err(e) = self.run_inner() {
            error!("{:?}", e);
            self.send_status(NodeStatus::Crashed).unwrap();
        }
    }

    fn run_inner(&mut self) -> Result<()> {
        if !self.meta.local && !self.task_image.starts_with("docker.io/library/") {
            self.send_status(NodeStatus::Pulling)?;
            let mut task = docker::pull(&self.task_image)?;

            // Allow client to abort during pull
            while task.try_wait()?.is_none() {
                if self.nd_dealer.poll(zmq::POLLIN, 500)? > 0 {
                    let msg = self.nd_dealer.recv_msg(0)?;
                    if &msg[..] == b"abort" {
                        debug!("Aborting docker pull task");
                        task.kill()?;
                        return Ok(());
                    }
                }
            }

            let res = task.wait_with_output()?;
            if !res.status.success() {
                bail!(
                    "Docker pull process failed. Stderr: {}",
                    String::from_utf8_lossy(&res.stderr)
                );
            }
        }

        let wk_broker_addr = if self.meta.broker_addr == "localhost" {
            // Set to docker internal ip for localhost
            "172.17.0.1"
        } else {
            &self.meta.broker_addr
        };

        // Spawn worker containers
        let exp_id_x = format!("{:x}", self.exp_id);
        let exp_id_label = format!("squid_exp_id={}", &exp_id_x);
        let exp_id_env = format!("SQUID_EXP_ID={}", &exp_id_x);
        let addr_env = format!("SQUID_ADDR={}", wk_broker_addr);
        let port_env = format!("SQUID_PORT={}", &self.port);
        let num_threads_env = format!("SQUID_NUM_THREADS={}", &self.meta.num_threads);
        if self.meta.test {
            docker::test_run(
                &self.task_image,
                &exp_id_label,
                &exp_id_env,
                &addr_env,
                &port_env,
                &num_threads_env,
            )?;
        } else {
            docker::run(
                &self.task_image,
                &exp_id_label,
                &exp_id_env,
                &addr_env,
                &port_env,
                &num_threads_env,
            )?;
        }

        self.send_status(NodeStatus::Active)?;

        Ok(())
    }

    fn send_status(&self, status: NodeStatus) -> Result<()> {
        debug!("Status: {:?}", status);
        self.nd_dealer
            .send_multipart(["status".as_bytes(), &[status as u8]], 0)?;

        Ok(())
    }
}
