# Squid ZMQ Multipart Protocols

### Client -> Broker

- ping
- run (id: `str`) (blueprint: `Blueprint`) (seeds: `Vec<Species>`)
- abort (id: `str`)

### Broker -> Client

- pong
- prog (id: `str`)
  - gen (num: `u32`)
    - running
    - done (evaluation: `PopEvaluation`)
  - agent (ix: `u32`)
    - running
    - done
    - failed (message: `str`)
- save (id: `str`)
  - population (agents: `Vec<json>`)
  - data (data: `json`)
- done (id: `str`)
- error (id: `str`) (message: `str`)

### Broker -> Manager

- spawn (id: `str`) (task_image: `str`)
- abort (id: `str`)

### Worker -> Broker

- (id: `str`) ready
- (id: `str`) done (gen: `u32`) (ix: `u32`) (fitness: `f64`)
- (id: `str`) moredata (gen: `u32`) (ix: `u32`) (data: `json`)
- (id: `str`) error (gen: `u32`) (ix: `u32`) (message: `str`)

### Broker -> Worker

- (id: `str`) sim (gen: `u32`) (ix: `u32`) (agent: `json`)
- (id: `str`) moredata (gen: `u32`) (ix: `u32`)
- (id: `str`) kill

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
