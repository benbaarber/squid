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
  - manager (mgid: `u32`) (status: `ManagerStatus`)
- save (id: `u64`)
  - population (agents: `Vec<json>`)
  - data (gen: `u32`) (data: `json`)
- done (id: `u64`)
- error (message: `str`)

### Broker -> Manager

- hb
- registered
- spawn (id: `u64`) (task_image: `str`)
- abort (id: `u64`)
- [TODO] meta (info req)

### Manager -> Broker

- hb
- register (num_workers: `u32`)
- status (status: `ManagerStatus`) 
- [TODO] meta (info res)

### Manager -> Worker

- hb

### Worker -> Manager

- hb
- register (id: `u64`)

### Worker -> Broker

- (id: `u64`) ready
- (id: `u64`) done (gen: `u32`) (ix: `u32`) (fitness: `f64`)
- (id: `u64`) moredata (gen: `u32`) (ix: `u32`) (data: `json`)
- (id: `u64`) error (gen: `u32`) (ix: `u32`) (message: `str`)

### Broker -> Worker

- (id: `u64`) sim (gen: `u32`) (ix: `u32`) (agent: `json`)
- (id: `u64`) moredata (gen: `u32`) (ix: `u32`)
- (id: `u64`) kill

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

5554 - manager (router) <- worker (dealer)
5555 - broker (router) <- client (dealer)
5556 - broker (router) <- manager (dealer)
5557+ - broker (router) <- worker (dealer)
