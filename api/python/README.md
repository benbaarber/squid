# Squid Simulation Runner API

A Python API for running simulations in the squid architecture using ZeroMQ.

## How It Works

The `squid.run(sim_fn)` function starts a simulation loop:
- Receives an agent from the Squid broker.
- Passes the agent (as a `dict`) to your simulation function `sim_fn`.
- Sends the simulation results (as a `dict`) back to the broker.

The loop terminates when there are no more agents to simulate.

## Usage

1. **Define Your Simulation Function**: 
   Your function must take a single agent `dict` and return a result `dict`.

   ```py
   def my_simulation(agent: dict) -> dict:
       # Process agent and return a result
       return {"fitness": agent["value"] * 2}
   ```

2. **Create the entrypoint**: 
   In your entrypoint file, call `squid.run()` with your simulation function:

   ```py
   import squid

   squid.run(my_simulation)
   ```

3. **Dockerize**: 
   More on this later

## Developers

To use the runner standalone, you will need to set the `SQUID_BROKER_ROUTER_URL` environment variable:
```bash
export SQUID_BROKER_ROUTER_URL="tcp://<broker-address>:<port>"
```

## Requirements

- Python 3.11+
- `pyzmq`
