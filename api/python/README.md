# Squid Worker API

A Python API for running simulations in the Squid architecture.

## How It Works

The `squid.run(sim_fn)` function starts a simulation loop:
- Receives an agent from the Squid broker.
- Passes the agent (as a `dict`) to your simulation function `sim_fn`.
- Sends the fitness score and optional additional simulation data (as a `dict`) back to the broker.

The loop terminates when there are no more agents to simulate.

## Installation

To install this package locally, first configure your shell environment (.env):

```sh
export GITLAB_PACKAGE_TOKEN='<your gitlab personal access token>' # must have 'api' scope
export SQUID_PACKAGE_URL="https://__token__:$GITLAB_PACKAGE_TOKEN@gitlab.com/api/v4/projects/64429395/packages/pypi/simple"
```

Source the environment variables, then run `pip install squid --index-url $SQUID_PACKAGE_URL`

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

### Publishing the package

To publish a new version of the package to the GitLab package registry, first configure your shell environment:

```sh
export WORKDIR="$(pwd)" # root of this repository

export TWINE_USERNAME='__token__'
export TWINE_PASSWORD='<your gitlab personal access token>' # must have 'api' scope
export TWINE_REPOSITORY_URL='https://gitlab.com/api/v4/projects/64429395/packages/pypi'

squid-python-publish() {
  cd $WORKDIR/api/python
  rm -r dist
  python -m build
  twine upload dist/*
  cd -
}
```

Then, bump the version of the package in the `pyproject.toml` file (bump according to semantic versioning).

Then simply run `squid-python-publish`.

### Running it standalone

To use the runner standalone, you will need to set the `SQUID_BROKER_WK_SOCK_URL` environment variable:
```bash
export SQUID_BROKER_WK_SOCK_URL="tcp://<broker-address>:5557"
```

Additionally, `squid.run()` expects the ID of the experiment to be passed as a command line argument. This is done automatically by the manager process when it spawns the containers, so currently, to use the runner standalone, you will have to start the experiment and then copy the ID and pass it as a command line argument manually.
