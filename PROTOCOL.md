# Squid ZMQ Multipart Protocols

### Client -> Broker

- ping
- status
- run (id: `u64`) (blueprint: `Blueprint`) (seeds: `Vec<Species>`)

### Broker -> Client

- pong
- status
  - idle
  - busy
- redirect (port: `u32`)
- error (message: `str`)

### Client -> Experiment Thread

- run (id: `u64`)
- abort (id: `u64`)

### Experiment Thread -> Client

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
- error (fatal: `bool`) (message: `str`)

### Broker -> Node

- hb
- registered
- spawn (id: `u64`) (task_image: `str`) (port: `u32`)
- abort (id: `u64`)
- [TODO] meta (info req)

### Node -> Broker

- hb
- register (num_workers: `u32`)
- status (id: `u64`) (status: `NodeStatus`)
- [TODO] meta (info res)

### Supervisor -> Experiment Thread

- (id: `u64`) register (wk_ids: `json [int]`)
- (id: `u64`) ok
- (id: `u64`) dead (dead_wk_ids: `json [int]`)
- (id: `u64`) error (msg: `str`)

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

### Broker -> Experiment Thread

- ndstatus (node_id: `u64`) (status: `u8`)

### Experiment Thread -> Broker

- ndspawn (task_image: `str`) (port: `u32`)
- ndabort

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

5600-5699 reserved
- (5600 + (n * 3)) - thread n (dealer) <- client (dealer)
- (5600 + (n * 3) + 1) - thread n (router) <- worker (dealer)
- (5600 + (n * 3) + 2) - thread n (router) <- supervisor (dealer)
for n in [0, 32]
