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

pub fn run(image: &str, broker_wk_env: &str, label: &str, id: &str) -> io::Result<Child> {
    Command::new("docker")
        .args([
            "run",
            "--rm",
            "-d",
            "-e",
            broker_wk_env,
            "-l",
            label,
            image,
            id,
        ])
        .spawn()
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
