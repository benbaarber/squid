# Squid ZMQ Multipart Protocols

### Client -> Broker

- ping
- status
- run (id: `u64`) (blueprint: `Blueprint`) (seeds: `Vec<Species>`) [NOTE: must use custom u64 identity]
- abort (id: `u64`)

### Broker -> Client

- pong
- status
  - idle
  - busy
- prog (id: `u64`)
  - gen (num: `u32`)
    - running
    - done (evaluation: `PopEvaluation`)
  - agent (ix: `u32`)
    - running
    - done
    - failed (message: `str`)
  - node (node_id: `u64`) (status: `NodeStatus`)
- save (id: `u64`)
  - population (agents: `Vec<json>`)
  - data (gen: `u32`) (data: `json`)
- done (id: `u64`)
- error (message: `str`)

### Broker -> Node

- hb
- registered
- spawn (id: `u64`) (task_image: `str`) (port: `str`)
- abort (id: `u64`)
- [TODO] meta (info req)

### Node -> Broker

- hb
- register (num_workers: `u32`)
- status (status: `NodeStatus`)
- [TODO] meta (info res)

### Supervisor -> Experiment Thread

- (id: `u64`) register (wk_ids: `json [int]`)
- (id: `u64`) ok
- (id: `u64`) dead (dead_wk_ids: `json [int]`) (new_wk_ids: `json [int]`)

### Experiment Thread -> Supervisor

- hb
- registered
- stop
- kill

### Worker -> Experiment Thread

- (id: `u64`) init
- (id: `u64`) sim (gen: `u32`) (ix: `u32`) (fitness: `f64`)
- (id: `u64`) data (gen: `u32`) (ix: `u32`) (data: `json`)
- (id: `u64`) error (gen: `u32`) (ix: `u32`) (message: `str`)

### Experiment Thread -> Worker

- sim (gen: `u32`) (ix: `u32`) (agent: `json`)
- data (gen: `u32`) (ix: `u32`)
- kill

### Client Broker thread -> TUI Thread

- gen (num: `u32`)
  - running
  - done (evaluation: `PopEvaluation`)
- agent (ix: `u32`)
  - running
  - done
  - failed (message: `str`)
- done
- crashed

## Ports

5555 - broker (router) <- client (dealer)
5556 - broker (router) <- node (dealer)

5600-5700 reserved:
5600 - broker experiment 1 thread (router) <- worker (dealer)
5601 - broker experiment 1 thread (router) <- worker master (dealer)
5602 - broker experiment 2 thread (router) <- worker (dealer)
5603 - broker experiment 2 thread (router) <- worker master (dealer)
...
