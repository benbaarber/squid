# Squid

## Installation

First, if you haven't already, [install Rust](https://www.rust-lang.org/tools/install).

If you've just installed Rust, run this to add `~/.cargo/bin` to your `$PATH`, otherwise you will not be able to use `cargo`.

```sh
echo '. "$HOME/.cargo/env"' >> ~/.bashrc && source ~/.bashrc
```

Then, install the CLI by running

```sh
cargo install --git https://github.com/benbaarber/squid.git squid
```

Run `squid` and you should see a friendly help message. Run `squid help <subcommand>` to learn how to use each subcommand.

## Overview: Squid Experiments

A Squid experiment is an evolutionary run. Squid provides a configurable
genetic algorithm and the networking/distribution, while the user provides the
simulation environment and the evaluation logic. In other words, you build a
function that accepts agent parameters, simulates that agent, collects data and
evaluates its fitness, and spits out the results. You plug that function into
squid and it uses it to evolve the population.

In order for Squid to parallelize your function across machines, you must
wrap the simulation environment in a docker image where the entrypoint is your
function passed to the squid worker API, which looks something like this in
Python:

```py
squid.worker(your_sim_fn)
```

Squid will take the provided `Dockerfile`, build the image, push it to your
docker registry, and tell other machines in the infrastructure to pull that
image down and run it. These containers connect to a central genetic algorithm
process, and agents are distributed among them to be simulated and evaluated in
parallel.

With that high level description in mind, let's go over the actual components of Squid.

## Architecture

The Squid architecture consists of a few key processes.

### Broker

The broker is the central process of Squid. It is responsible for spawning
experiment threads that run the GA and send experimental data back to the
client. It is also responsible for keeping long-lived nodes alive via heartbeating.

### Node

Many nodes are connected to the broker. There is one node process per machine
in the squid infrastructure, so the machines themselves can also be referred to
as squid nodes in this context. The node process is responsible for spawning or
killing docker containers for experiments at the broker's command.

### Worker

The worker process is what runs inside of the experiment container provided by
the user. There are many workers per node, and the number of workers spawned
per experiment can be configured for each node. Each worker process connects to
the GA thread on the broker directly, and for the duration of the experiment,
it accepts agent parameters, runs them through the user's simulation, and sends
the results back. As such, the worker process is ephemeral and only lives as
long as a single experiment.

The worker operates by providing an API for the user to pass a simulation
function. This design of accepting an arbitrary function and running inside an
arbitrary simulation environment allows Squid to treat the worker as a black
box function that takes agents as input and provides fitness and experimental
data as output. This allows the user complete freedom for the simulation
methodology and fitness evaluation.

### Supervisor

The supervisor is another ephemeral process with the same lifecycle as the
worker. Its only responsibility is monitoring workers on a single node,
heartbeating with the GA thread on the broker to communicate the health of
workers so the system can be reactive in requeueing agents on workers that have
silently failed.

### Client

This is the process users interface with to initiate experiments. It is invoked
via `squid run`, and is responsible for reading the configuration file
(`squid.toml`), serializing optional seed agents, communicating with the broker
to spawn an experiment thread, storing data, and visualizing experiment
progress via a TUI (terminal user interface).

## Project Structure

You can use `squid init` to generate a directory with a template experiment
setup, including a `Dockerfile`, `squid.toml`, and a python entrypoint, or you
can add these files to an existing project.

---

### `Dockerfile`

When creating a new Squid experiment, the first thing to do is get your
simulation environment together and start developing your docker image.

**NOTE:** Unless you are only running Squid locally, it is important to learn
some more advanced Docker concepts, particularly how to optimize for image size
and build time (see [this
article](https://devopscube.com/reduce-docker-image-size/)). Since these images
must be built frequently and pushed/pulled from a container registry,
learning and implementing these techniques will significantly improve the
performance and cost of a Squid experiment.

---

### `squid.toml`

All configuration of Squid's genetic algorithm and other internals is done
through this toml file. See the specification [here](./example.squid.toml).

---

Aside from these two files, the structure of the project is entirely up to you.
As long as the worker is initialized at some point in the entrypoint of the
docker image and the csv data matches up, Squid is cool with you no matter how
weird your code is.

## Setting Up Infrastructure

- **Broker**: Run `squid broker`
- **Node**: Run `squid node -b <broker-addr> ...opts`

## Workflow

### Test Mode

The Squid CLI has a test mode (`squid run -t`) that should _always_ be used
before deploying an experiment, locally or not. Test mode will spawn a local
broker and node on your machine and run one generation (ignoring your
`squid.toml`) with one agent by default (which can be overridden with the `-n
<number>` flag). This functions as a quick integration test of your experiment.
It will parse your config, spawn your container, simulate an agent, and
validate your csv data. It will flush the output of the container directly to
your terminal, allowing for easy debugging. If any steps fail (they will), you
can catch them before starting an experiment and walking away. Only when all
tests pass should you deploy your experiment.

### Local Mode

Squid also has a local mode (`squid run -l`) that will also spawn the broker
and node locally, but in this case it will go through with your entire
experiment on your machine. It will not push or pull from the container
registry. Local mode is useful for smaller scale experiments and additional
testing. You can also configure the number of parallel workers to spawn with
the `-n <number>` option.

### Remote Mode

The most common scenario will be where the squid broker and nodes are deployed
on an existing infrastructure, and an experiment needs to be initiated from a
remote device. In this case, either pass the broker address to the command
(`squid run -b <broker-addr>`), or set the environment variable
`SQUID_BROKER_ADDR` to the same value.

In this mode, since the client does not need to be connected for the experiment
to progress, the client can detach from the experiment at any point after
initializing it. In the TUI, hit `d` to detach. To reattach at a later point,
use `squid attach <experiment-id> <path>`, where the path is the parent path
you want to dump the experiment outdir to. To fetch experimental data from the
broker after the experiment has already ended, use `squid fetch <experiment-id>
<path>`.

## Worker Installation

```sh
pip install 'git+https://github.com/benbaarber/squid-worker.git'
```

For more specific documentation on how to use the worker, including the
ROS-specific version, go [here](https://github.com/benbaarber/squid-worker.git).

