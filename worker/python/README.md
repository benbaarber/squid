# Squid Worker API

A Python API for running simulations in the Squid architecture.

## How It Works

The `squid.run(sim_fn)` function starts a simulation loop:
- Receives an agent from the Squid broker.
- Passes the agent (as a `dict`) to your simulation function `sim_fn`.
- Sends the fitness score and optional additional simulation data (as a `dict`) back to the broker.

The loop terminates when there are no more agents to simulate.

## Installation

### With SSH

If your ssh key is linked to gitlab, and you have your ssh-agent set up, simply run:

```sh
pip install "git+ssh://git@gitlab.com/VivumComputing/scientific/robotics/dnfs/evolution/squid.git#egg=squid&subdirectory=api/python"
```

### With HTTPS

First configure your shell environment:

```sh
export GITLAB_TOKEN='<your gitlab personal access token>' # must have at least `read_repository` scope
```

Then run:

```sh
pip install "git+https://${GITLAB_TOKEN}@gitlab.com/VivumComputing/scientific/robotics/dnfs/evolution/squid.git#egg=squid&subdirectory=api/python"
```

## Usage

1. **Define Your Simulation Function**: 
   Your function must take a single agent `dict` and return a fitness score `float` and an optional `dict` for additional simulation data. 

   The dict should mirror the `[csv_data]` section in your blueprint file and take the form:
   ```py
   {
      "csv_data_key": [row1, row2, ...],
      ...
   }
   ```
   where the rows are lists of floats that line up with the headers specified in the blueprint.
   These rows will be appended to the csv files for the best agent after each generation.

   In other words, your function must match this signature:

   ```py
   def my_simulation(agent: dict) -> tuple[float, dict[str, list[list[float]]] | None]
   ```

   An example:

   ```py
   def my_simulation(agent):
       # Process agent and return a result
       fitness = 12.4
       sim_data = {
         "sensor_outputs": [
            [0.1, 0.2, 0.3, 0.4], 
            [0.5, 0.6, 0.7, 0.8]
         ]
       }

       return fitness, sim_data
   ```

2. **Create the entrypoint**: 
   In your entrypoint file, call `squid.run()` with your simulation function:

   ```py
   import squid

   ...

   if __name__ == "__main__":
      squid.run(my_simulation)
   ```

## Requirements

- Python 3.11+
- `pyzmq`
- `orjson`

## Development

### Running it standalone

To use the runner standalone, you will need to set the `SQUID_BROKER_WK_SOCK_URL` environment variable:
```bash
export SQUID_BROKER_WK_SOCK_URL="tcp://<broker-address>:5557"
```

Additionally, `squid.run()` expects the ID of the experiment to be passed as a command line argument. This is done automatically by the manager process when it spawns the containers, so currently, to use the runner standalone, you will have to start the experiment and then copy the ID and pass it as a command line argument manually.
