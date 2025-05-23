#![allow(unused)]
use core::str;
use std::{
    io,
    process::{Child, Command, ExitStatus, Stdio},
};

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
    exp_label: &str,
    exp_id_env: &str,
    addr_env: &str,
    port_env: &str,
    num_threads_env: &str,
) -> io::Result<Child> {
    Command::new("docker")
        .args([
            "run",
            "--rm",
            "-d",
            "-l",
            exp_label,
            "-e",
            exp_id_env,
            "-e",
            addr_env,
            "-e",
            port_env,
            "-e",
            num_threads_env,
            image,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
}

pub fn test_run(
    image: &str,
    exp_label: &str,
    exp_id_env: &str,
    addr_env: &str,
    port_env: &str,
    num_threads_env: &str,
) -> io::Result<()> {
    Command::new("docker")
        .args([
            "run",
            "--rm",
            "-l",
            exp_label,
            "-e",
            exp_id_env,
            "-e",
            addr_env,
            "-e",
            port_env,
            "-e",
            num_threads_env,
            image,
        ])
        .spawn()?;

    // debug!(
    //     "Worker stdout:\n{}",
    //     String::from_utf8_lossy(&output.stdout)
    // );
    // debug!(
    //     "Worker stderr:\n{}",
    //     String::from_utf8_lossy(&output.stderr)
    // );

    Ok(())
}

pub fn ps_by_exp(exp_label_query: &str) -> io::Result<Vec<String>> {
    let output = Command::new("docker")
        .args([
            "ps",
            "-f",
            exp_label_query,
            "--format",
            "{{.Label \"squid_worker_id\"}}",
        ])
        .output()?;
    Ok(String::from_utf8_lossy(&output.stdout)
        .trim()
        .split("\n")
        .map(|s| s.to_owned())
        .collect::<Vec<_>>())
}

pub fn kill_by_exp(exp_label_query: &str) -> io::Result<()> {
    let output = Command::new("docker")
        .args(["ps", "-q", "-f", exp_label_query])
        .output()?;
    let container_ids = String::from_utf8_lossy(&output.stdout);

    if container_ids.trim().len() > 0 {
        Command::new("docker")
            .arg("kill")
            .args(container_ids.split_whitespace())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?
            .wait()?;
    }

    Ok(())
}
