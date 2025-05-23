import math
from multiprocessing import Process
import random

import orjson
import zmq
from squid.util import *


def _worker_proc(sim_fn: SimFn, wk_id: int, exp_id_b: bytes, wk_router_url: str):
    ctx = zmq.Context()
    ga_sock = ctx.socket(zmq.DEALER)
    ga_sock.setsockopt(zmq.IDENTITY, se_u64(wk_id))
    ga_sock.connect(wk_router_url)

    cur_gen = 0
    best_ix = 0
    best_fitness = -math.inf
    best_data = None
    agent_ix_b = None

    try:
        ga_sock.send_multipart([exp_id_b, b"init"])

        while True:
            msgb = ga_sock.recv_multipart()

            cmd = msgb[0]
            match cmd:
                case b"sim":
                    gen_num_b = msgb[1]
                    gen_num = de_i32(gen_num_b)
                    agent_ix_b = msgb[2]
                    genome = msgb[3].decode()

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
    except Exception as e:
        ga_sock.send_multipart(
            [exp_id_b, b"error", se_u32(cur_gen), agent_ix_b, str(e).encode()]
        )
        raise e
    finally:
        ctx.destroy(linger=0)


def _supervisor(proc_factory: WorkerFactory):
    exp_id_b = se_u64(int(get_env_or_throw("SQUID_EXP_ID"), base=16))
    broker_addr = get_env_or_throw("SQUID_ADDR")
    port = int(get_env_or_throw("SQUID_PORT"))
    num_procs = int(get_env_or_throw("SQUID_NUM_THREADS"))

    wk_url = f"tcp://{broker_addr}:{port+1}"
    sv_url = f"tcp://{broker_addr}:{port+2}"

    sv_id = random.getrandbits(64)

    ctx = zmq.Context()

    ga_sock = ctx.socket(zmq.DEALER)
    ga_sock.setsockopt(zmq.IDENTITY, se_u64(sv_id))
    ga_sock.connect(sv_url)

    try:
        worker_ids = [random.getrandbits(64) for _ in range(num_procs)]
        ga_sock.send_multipart([exp_id_b, b"register", orjson.dumps(worker_ids)])

        while ga_sock.poll(5000):
            msgb = ga_sock.recv_multipart()
            cmd = msgb[0]
            match cmd:
                case b"registered":
                    print("Registered with broker")
                    break
                case b"stop" | b"kill":
                    ctx.destroy(linger=0)
                    return
        else:
            print("Broker did not respond. Terminating.")
            ctx.destroy(linger=0)
            return

        workers = proc_factory(exp_id_b, wk_url, worker_ids)

        BK_TTL = 15000
        while ga_sock.poll(BK_TTL):
            msgb = ga_sock.recv_multipart()
            cmd = msgb[0]
            match cmd:
                case b"hb":
                    dead = [wk_id for wk_id, p in workers.items() if not p.is_alive()]
                    if len(dead) > 0:
                        for wk_id in dead:
                            del workers[wk_id]
                        # TODO respawn maybe?
                        ga_sock.send_multipart([exp_id_b, b"dead", orjson.dumps(dead)])
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
            print("Broker went offline. Terminating.")
            for p in workers.values():
                p.kill()
    except Exception as e:
        ga_sock.send_multipart([exp_id_b, b"error", str(e).encode()])
        raise e
    finally:
        ctx.destroy(linger=0)


def worker(sim_fn: SimFn):
    """
    Run the Squid worker.

    This function must be called in the entrypoint of the simulation docker image.

    :param sim_fn:
        A callable that takes an agent's parameters as a JSON string and returns a fitness score and an optional data dict.
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

    def proc_factory(exp_id_b, wk_router_url, wk_ids):
        workers = {}
        for wk_id in wk_ids:
            p = Process(
                target=_worker_proc,
                args=(sim_fn, wk_id, exp_id_b, wk_router_url),
                name=f"squid_sim_proc_{wk_id:x}",
            )
            workers[wk_id] = p
            p.start()
        return workers

    _supervisor(proc_factory)
