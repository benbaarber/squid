pub fn make_dotenv(gitlab_token: &str) -> String {
    format!(
        r#"export GITLAB_TOKEN='{}'
export SQUID_BROKER_URL='tcp://localhost:5555'
"#,
        gitlab_token
    )
}

pub const BLUEPRINT: &str = r#"[experiment]
task_image = "docker.io/library/example"
genus = "CTRNN"
out_dir = "./agents"

[species]
input_size = 7
hidden_size = 2
output_size = 4
step_size = 0.1
tau_bounds = [1e-5, 16]
weight_bounds = [-16, 16]
bias_bounds = [-16, 16]
gain_bounds = [1e-5, 16]

[ga]
population_size = 100
num_generations = 100
elitism_percent = 0.10
random_percent = 0
mutation_chance = 0.1
mutation_magnitude = 1
save_every = 10
save_percent = 0.10

[csv_data]
data = ["h1", "h2", "h3"]
"#;

pub const DOCKERFILE: &str = r#"FROM python:3.11

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY simulation/ simulation/

ENV SQUID_BROKER_WK_SOCK_URL='tcp://172.17.0.1:5557'
ENV PYTHONPATH='/app'

ENTRYPOINT [ "python", "simulation/main.py" ]
"#;

pub const REQUIREMENTSTXT: &str = "git+https://${GITLAB_TOKEN}@gitlab.com/VivumComputing/scientific/robotics/dnfs/evolution/squid.git#egg=squid&subdirectory=api/python\n";

pub const MAINPY: &str = r#"import squid


def simulation(agent):
    fitness = 42
    data = {"data": [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]]}

    return fitness, data


if __name__ == "__main__":
    squid.run(simulation)
"#;

pub const GITIGNORE: &str = ".env\nagents/\n__pycache__/\nvenv/\n";
