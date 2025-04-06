import math
from multiprocessing import Process
import random
import sys

import orjson
import zmq
from squid.util import *


def _worker(sim_fn: SimFn, worker_url: str, exp_id_b: bytes, wk_id: int):
    ctx = zmq.Context()
    ga_sock = ctx.socket(zmq.DEALER)
    ga_sock.setsockopt(zmq.IDENTITY, se_u64(wk_id))
    ga_sock.connect(worker_url)

    try:
        ga_sock.send_multipart([exp_id_b, b"init"])

        cur_gen = 0
        best_ix = 0
        best_fitness = -math.inf
        best_data = None

        while True:
            msgb = ga_sock.recv_multipart()

            cmd = msgb[0]
            match cmd:
                case b"sim":
                    gen_num_b = msgb[1]
                    gen_num = de_i32(gen_num_b)
                    agent_ix_b = msgb[2]
                    genome = orjson.loads(msgb[3])

                    fitness, data = sim_fn(genome)

                    if gen_num != cur_gen:
                        cur_gen = gen_num
                        best_ix = 0
                        best_fitness = -math.inf
                        best_data = None

                    if fitness > best_fitness:
                        best_fitness = fitness
                        best_data = data
                        best_ix = de_i32(agent_ix_b)

                    ga_sock.send_multipart(
                        [exp_id_b, b"sim", gen_num_b, agent_ix_b, se_f64(fitness)]
                    )
                case b"data":
                    gen_num_b = msgb[1]
                    gen_num = de_i32(gen_num_b)
                    agent_ix_b = msgb[2]
                    agent_ix = de_i32(agent_ix_b)

                    if gen_num != cur_gen:
                        print(
                            f"Bad data request - generation mismatch, expected gen was {cur_gen}, requested gen {gen_num}"
                        )

                    if agent_ix != best_ix:
                        print(
                            f"Bad data request - agent index mismatch, best index was {best_ix}, requested index {agent_ix}"
                        )

                    ga_sock.send_multipart(
                        [
                            exp_id_b,
                            b"data",
                            gen_num_b,
                            agent_ix_b,
                            orjson.dumps(best_data),
                        ]
                    )
                case b"kill":
                    break
    finally:
        ctx.destroy()


def run(sim_fn: SimFn):
    """
    Run the simulation loop.

    This function should be called in the entrypoint of the simulation docker image.

    :param sim_fn:
        A callable that takes an agent (dict) and returns a fitness score and an optional data dict.
        The dict should mirror the `[csv_data]` section in your blueprint file and take the form:
        ```
        {
            "csv_data key": [row1, row2, ...],
            ...
        }
        ```
        where the rows are lists of floats that line up with the headers specified in the blueprint.
        These rows will be appended to the csv files for the best agent after each generation.
    """
    exp_id_b = se_u64(int(sys.argv[1], base=16))
    broker_url = sys.argv[2]
    port = int(sys.argv[3])
    num_procs = int(sys.argv[4])

    if not callable(sim_fn):
        raise ValueError(
            "The sim_fn argument must be a callable that accepts a dict and returns a dict."
        )

    wk_url = f"{broker_url}:{port}"
    sv_url = f"{broker_url}:{port+1}"

    sv_id = random.getrandbits(64)

    ctx = zmq.Context()

    ga_sock = ctx.socket(zmq.DEALER)
    ga_sock.setsockopt(zmq.IDENTITY, se_u64(sv_id))
    ga_sock.connect(sv_url)

    worker_ids = [random.getrandbits(64) for _ in range(num_procs)]
    workers: dict[int, Process] = {}

    try:
        ga_sock.send_multipart(
            [exp_id_b, b"register", orjson.dumps(worker_ids)]
        )
        
        while ga_sock.poll(5000):
            msgb = ga_sock.recv_multipart()
            cmd = msgb[0]
            match cmd:
                case b"registered":
                    print("Registered with broker")
                    break
                case b"stop" | b"kill":
                    ctx.destroy()
                    return
        
        for wk_id in worker_ids:
            p = Process(
                target=_worker,
                args=(sim_fn, wk_url, exp_id_b, wk_id),
                name=f"squid_sim_proc_{wk_id:x}",
            )
            workers[wk_id] = p
            p.start()

        BK_TTL = 15000

        while ga_sock.poll(BK_TTL):
            msgb = ga_sock.recv_multipart()
            cmd = msgb[0]
            match cmd:
                case b"hb":
                    dead = [wk_id for wk_id, p in workers.items() if not p.is_alive()]
                    if len(dead) > 0:
                        new = []
                        for wk_id in dead:
                            del workers[wk_id]
                            new_wk_id = random.getrandbits(64)
                            p = Process(
                                target=_worker,
                                args=(sim_fn, wk_url, exp_id_b, new_wk_id),
                                name=f"squid_sim_proc_{new_wk_id:x}",
                            )
                            workers[new_wk_id] = p
                            new.append(new_wk_id)
                            p.start()
                        ga_sock.send_multipart(
                            [exp_id_b, b"dead", orjson.dumps(dead), orjson.dumps(new)]
                        )
                    else:
                        ga_sock.send_multipart([exp_id_b, b"ok"])
                case b"registered":
                    ...
                case b"stop":
                    for p in workers.values():
                        p.join()
                    break
                case b"kill":
                    for p in workers.values():
                        p.kill()
                    break
        else:
            print("Broker went offline, terminating...")
            for p in workers.values():
                # TODO verify terminate works
                p.terminate()
    finally:
        ctx.destroy()
