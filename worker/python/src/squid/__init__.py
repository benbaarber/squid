import os
import struct
import sys
from typing import Callable
import orjson
import zmq


def run(sim_fn: Callable[[dict], tuple[float, dict | None]]):
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
    exp_id_b = struct.pack("<Q", int(sys.argv[1], base=16))

    broker_url = os.environ.get("SQUID_BROKER_WK_SOCK_URL")
    if broker_url is None:
        raise EnvironmentError(
            "Environment variable 'SQUID_BROKER_WK_SOCK_URL' is undefined."
        )

    if not callable(sim_fn):
        raise ValueError(
            "The sim_fn argument must be a callable that accepts a dict and returns a dict."
        )

    ctx = zmq.Context()
    mg_sock = ctx.socket(zmq.DEALER)
    ga_sock = ctx.socket(zmq.DEALER)
    
    try:
        mg_sock.setsockopt(zmq.LINGER, 0)
        mg_sock.connect("tcp://172.17.0.1:5554")
        mg_sock.send_multipart([b"register", exp_id_b])
        
        ga_sock.connect(broker_url)
        ga_sock.send_multipart([exp_id_b, b"ready"])

        current_gen = 0
        best_agent_fitness = 0.0
        best_agent_data = None

        poller = zmq.Poller()
        poller.register(mg_sock, zmq.POLLIN)
        poller.register(ga_sock, zmq.POLLIN)
        
        active = True
        while active:
            active = False
            socks: dict[zmq.SyncSocket, int] = dict(poller.poll(3e5))

            if mg_sock in socks:
                active = True
                msgb = mg_sock.recv()
                if msgb == b"hb":
                    mg_sock.send(b"hb")
            
            if ga_sock in socks:
                active = True
                msgb = ga_sock.recv_multipart()
                cmd = msgb[1]
                match cmd:
                    case b"sim":
                        try:
                            gen: int = struct.unpack("<I", msgb[2])
                            if gen != current_gen:
                                best_agent_fitness = 0.0
                                best_agent_data = None
                                current_gen = gen

                            agent = orjson.loads(msgb[4])
                            fitness, data = sim_fn(agent)
                            if fitness > best_agent_fitness:
                                best_agent_fitness = fitness
                                best_agent_data = data
                            fitness_b = struct.pack("<d", fitness)
                            ga_sock.send_multipart(
                                [
                                    exp_id_b,
                                    b"done",
                                    msgb[2],
                                    msgb[3],
                                    fitness_b,
                                ]
                            )
                            ga_sock.send_multipart([exp_id_b, b"ready"])
                        except Exception as e:
                            ga_sock.send_multipart(
                                [
                                    exp_id_b,
                                    b"error",
                                    msgb[2],
                                    msgb[3],
                                    f"Worker error (sim): {e}".encode(),
                                ]
                            )
                            print("Simulation error:", str(e))
                    case b"moredata":
                        try:
                            gen: int = struct.unpack("<I", msgb[2])
                            if gen != current_gen:
                                raise ValueError(
                                    f"moredata generation mismatch from broker, expected gen {current_gen}, got gen {gen}"
                                )

                            data_b = orjson.dumps(best_agent_data)
                            ga_sock.send_multipart(
                                [exp_id_b, b"moredata", msgb[2], msgb[3], data_b]
                            )
                        except Exception as e:
                            ga_sock.send_multipart(
                                [
                                    exp_id_b,
                                    b"error",
                                    msgb[2],
                                    msgb[3],
                                    f"Worker error (moredata): {e}".encode(),
                                ]
                            )
                            print("Error:", e)
                    case b"kill":
                        break
                    case _:
                        print("Received invalid command:", cmd)
        
        mg_sock.send(b"drop")
    finally:
        mg_sock.close()
        ga_sock.close()
        ctx.term()
