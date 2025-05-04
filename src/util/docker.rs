use core::str;
use std::{
    io,
    process::{Child, Command, ExitStatus, Stdio},
};

use tracing::debug;

pub fn is_installed() -> io::Result<bool> {
    let result = Command::new("which")
        .arg("docker")
        .stdout(Stdio::null())
        .status()?;
    Ok(result.success())
}

pub fn build(image: &str, path: &str) -> io::Result<ExitStatus> {
    Command::new("docker")
        .args(["build", "--ssh", "default", "-t", image, path])
        .env("DOCKER_BUILDKIT", "1")
        .spawn()?
        .wait()
}

pub fn push(image: &str) -> io::Result<ExitStatus> {
    Command::new("docker").args(["push", image]).spawn()?.wait()
}

pub fn pull(image: &str) -> io::Result<ExitStatus> {
    Command::new("docker").args(["pull", image]).spawn()?.wait()
}

pub fn run(
    image: &str,
    id_hex: &str,
    broker_addr: &str,
    port: &str,
    num_threads: &str,
) -> io::Result<Child> {
    let label = "squid_id=".to_string() + id_hex;
    let id_hex_env = "SQUID_EXP_ID=".to_string() + id_hex;
    let addr_env = "SQUID_ADDR=".to_string() + broker_addr;
    let port_env = "SQUID_PORT=".to_string() + port;
    let num_threads_env = "SQUID_NUM_THREADS=".to_string() + num_threads;

    Command::new("docker")
        .args([
            "run",
            "--rm",
            "-d",
            "-l",
            &label,
            "-e",
            &id_hex_env,
            "-e",
            &addr_env,
            "-e",
            &port_env,
            "-e",
            &num_threads_env,
            image,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
}

pub fn test_run(
    image: &str,
    id_hex: &str,
    broker_addr: &str,
    port: &str,
    num_threads: &str,
) -> io::Result<()> {
    let label = "squid_id=".to_string() + id_hex;
    let id_hex_env = "SQUID_EXP_ID=".to_string() + id_hex;
    let addr_env = "SQUID_ADDR=".to_string() + broker_addr;
    let port_env = "SQUID_PORT=".to_string() + port;
    let num_threads_env = "SQUID_NUM_THREADS=".to_string() + num_threads;

    let output = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-l",
            &label,
            "-e",
            &id_hex_env,
            "-e",
            &addr_env,
            "-e",
            &port_env,
            "-e",
            &num_threads_env,
            image,
        ])
        .output()?;

    debug!(
        "Worker stdout:\n{}",
        String::from_utf8_lossy(&output.stdout)
    );
    debug!(
        "Worker stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    Ok(())
}

pub fn kill_all(id: &str) -> io::Result<()> {
    let label_query = format!("label=squid_id={}", id);

    let output = Command::new("docker")
        .args(["ps", "-q", "-f", &label_query])
        .output()?;
    let container_ids = String::from_utf8_lossy(&output.stdout);

    if container_ids.trim().len() > 0 {
        Command::new("docker")
            .arg("kill")
            .args(container_ids.split_whitespace())
            .spawn()?
            .wait()?;
    }

    Ok(())
}
