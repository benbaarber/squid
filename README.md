# Squid

Squid is an asynchronous neuroevolution architecture built using ZeroMQ and written in Rust. It enables massively parallel execution of evolution experiments across multiple machines and scales effortlessly. Squid is modular, allowing flexibility and extensibility to suit a variety of use cases.

## Overview

Squid consists of four core components:

1. **Broker** - Manages the evolution process and distributes work.
2. **Node** - Handles containerized workers on squid nodes.
3. **Worker** - Runs simulations for agents and calculates fitness.
4. **Client (CLI)** - Initiates and monitors experiments.

Each component has a specific role in the architecture, contributing to Squid's scalability and modularity.

## Components

### Broker
- **Type**: Long-running, experiment-agnostic process.
- **Responsibilities**:
  - Listens for experiment requests from the client.
  - Manages the genetic algorithm, evolving and evaluating the population
  - Distributes agents to workers using a load-balancing pattern.
  - Acts as the central hub of Squid's network topology.
- **Current Limitations**:
  - Supports only one experiment at a time.
- **Deployment**:
  - Runs on a single machine at a static address.

---

### Node
- **Type**: Long-running, experiment-agnostic process.
- **Responsibilities**:
  - Spawns and manages task containers upon requests from the broker.
  - Facilitates parallel execution by creating multiple workers.
- **Deployment**:
  - Deployed on one or more worker host machines.

---

### Worker
- **Type**: Short-lived, experiment-specific process.
- **Responsibilities**:
  - Runs inside a containerized environment to process agents.
  - Receives agents from the broker, runs simulations, computes fitness, and aggregates additional simulation data.
  - Sends fitness scores and additional data back to the broker in a loop until the experiment is complete.
- **Requirements**:
  - A Docker image configured with:
    - A main executable implementing the Squid Worker API.
    - The executable set as the Docker image's entrypoint.
  - The rest of the logic and implementation is entirely user-defined and opaque to the architecture.
- **Deployment**:
  - Spawned and terminated by the node process on worker hosts.

---

### Client (CLI)
- **Type**: User-facing entrypoint.
- **Responsibilities**:
  - Validates and initiates experiments by communicating with the broker.
  - Displays a Terminal User Interface (TUI) with live progress updates.
  - Saves simulation data and populations to disk.
  - Synchronizes with the broker to maintain experiment progress.
- **Deployment**:
  - Runs on the user's local machine.

## Installation

To use Squid, install the Squid client CLI.

First, if you haven't already, [install Rust](https://www.rust-lang.org/tools/install).

Then, install the CLI by running
```sh
cargo install --git ssh://git@gitlab.com/VivumComputing/scientific/tools/squid.git squid
```

If that fails, ensure you have an SSH key registered with GitLab and that it is loaded into your ssh-agent.

## Workflow

1. **Build Your Simulation**:
   - Build the simulation that will run in the Squid workers
   - Iteratively update your squid.toml (the experiment configuration or "blueprint") and simulation logic
   - Keep your CSV data format synchronized with your logic

2. **Set Up and Validate Experiment**:
   - Prepare the Docker image for your worker
   - Validate the blueprint with the client CLI

3. **Run the Experiment**:
   - Set the SQUID_BROKER_WK_SOCK_URL environment variable to point at the broker (port 5557)
   - Initiate the experiment using the client: `squid run`

4. **Monitor Progress**:
   - Use the TUI provided by the client to track progress in real time while the client collects results and simulation data

5. **Refine and Iterate**:
   - Use the results to refine your simulation logic or configuration
   - Repeat the process for improved outcomes
