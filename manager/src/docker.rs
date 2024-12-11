use std::{
    io,
    process::{Child, Command},
};

pub fn is_installed() -> io::Result<bool> {
    let result = Command::new("which").arg("docker").status()?;
    Ok(result.success())
}

pub fn pull(image: &str) -> io::Result<bool> {
    let result = Command::new("docker")
        .args(["pull", image])
        .spawn()?
        .wait()?;

    Ok(result.success())
}

pub fn run(image: &str, label: &str, id: &str) -> io::Result<Child> {
    Command::new("docker")
        .args(["run", "--rm", "-d", "-l", label, image, id])
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
