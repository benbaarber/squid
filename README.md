# Squid

### Server -> Broker protocol

start (id) (blueprint)

abort (id)

### Broker -> Server protocol

error (id) (message)

### Broker -> Worker protocol

spawn (container uri)

### Simulation -> Broker protocol

ready

done (ix) (results)
ix: a u64 index of the agent in the population array
results: simulation results
{
  score: f64
}

error (ix) (message)

### Broker -> Simulation protocol

sim (ix) (agent)
start a sim with serialized agent

kill
end process
