# Squid

### Server -> Broker protocol

- ping
- run (id) (blueprint)
- abort (id)

### Broker -> Server protocol

- pong
- progress (id)
  - gen (num)
  - agent (ix)
    - running
    - done
    - failed (message)
- done (id) (summary)
- error (id) (message)

### Broker -> Worker protocol

- spawn (id) (task image)
- abort (id)

### Simulation -> Broker protocol

- ready
- done (ix) (results)
- error (ix) (message)

### Broker -> Simulation protocol

- sim (ix) (agent)
- kill
