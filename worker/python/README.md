# Squid Worker

The Squid worker python API

## How It Works

The `squid.worker(sim_fn)` function starts a simulation loop:
- Receives an agent from the GA thread running on the Squid broker.
- Passes the agent's parameters as a JSON string to your simulation function `sim_fn`.
- Sends the fitness score and optional additional simulation data (as a `dict`) back to the broker.

The loop terminates when there are no more agents to simulate.

## Installation

```sh
pip install 'git+ssh://git@gitlab.com/VivumComputing/scientific/tools/squid.git#egg=squid&subdirectory=worker/python'
```

## Usage

### Generic Worker

The generic worker takes in a simulation function and runs it as it receives agents from the GA.

1. **Define your simulation function**

   Your function must take a genome JSON string and return a fitness score `float` and an optional `dict` for additional simulation data. 

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
   def my_simulation(genome: str) -> tuple[float, dict[str, list[list[float]]] | None]
   ```

   An example:

   ```py
   def my_simulation(genome):
      # Deserialize genome
      ctrnn = CTRNN(7, 4, 2)
      ctrnn.load_genome_json(genome)

      # Simulate
      ...

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

2. **Create the entrypoint**

   In your entrypoint file, call `squid.worker()` with your simulation function:

   ```py
   import squid

   ...

   if __name__ == "__main__":
      squid.worker(my_simulation)
   ```

### ROS Worker

The ROS worker is an extension of the Squid worker that is specific to running a multiagent simulation in ROS and Gazebo. It takes in a class that inherits from `squid.ros.AgentNode` (which in turn inherits from the ROS `Node` class) as well as the SDF string of the model to be simulated. 

1. **Define your node class**

   In the same way you start a python ROS node project, begin by creating a node class. However, instead of inheriting from rclpy's `Node`, inherit from squid's `AgentNode`.

   ```py
   import squid.ros

   class ExampleNode(squid.ros.AgentNode):
      def __init__(self, **kwargs):
         super().__init__(**kwargs)
         
         # ROS node setup
         ...
      
      # ROS node callbacks
      ...
   ```

2. **Implement the `sim` method**

   `AgentNode` also defines one abstract method called `sim`. This method has the same signature as the simulation function in the [generic worker](#generic-worker), and is called the same way internally.

   ```py
   import squid.ros
   import rclpy

   class ExampleNode(squid.ros.AgentNode):
      ...

      def sim(self, genome: str) -> tuple[float, dict[str, list[list[float]]] | None]:
         # Deserialize genome
         self.ctrnn.load_genome_json(genome)

         # Reset drone
         self.reset()

         # Spin ros loop until exit condition is met, and collect data for fitness and evaluation
         self.sim_done = False
         while not self.sim_done:
            rclpy.spin_once(self, timeout_sec=0.1)

         # Evaluate agent
         fitness = self.compute_fitness()

         return fitness, self.data
   ```

3. **Create the entrypoint**

   ```py
   def main():
      sdf = open("models/my_model/model.sdf").read()
      squid.ros.ros_worker(ExampleNode, sdf)
   ```

## Requirements

### Base

- Python 3.11+
- pyzmq
- orjson

### ROS

- rclpy