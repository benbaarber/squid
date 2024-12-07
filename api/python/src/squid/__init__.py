import json
import os
import struct
import sys
from typing import Callable
import zmq


def run(sim_fn: Callable[[dict], tuple[float, dict[str, list[list[float]]] | None]]):
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
    exp_id_b = sys.argv[1].encode()

    broker_url = os.environ.get("SQUID_BROKER_ROUTER_URL")
    if broker_url is None:
        raise EnvironmentError(
            "Environment variable 'SQUID_BROKER_ROUTER_URL' is undefined."
        )

    if not callable(sim_fn):
        raise ValueError(
            "The sim_fn argument must be a callable that accepts a dict and returns a dict."
        )

    ctx = zmq.Context()
    socket = ctx.socket(zmq.DEALER)

    try:
        socket.connect(broker_url)
        socket.send_multipart([exp_id_b, b"ready"])

        current_gen = 0
        best_agent_fitness = 0.0
        best_agent_data = None

        # TODO: for now shutting down after 5 minutes of no activity
        # Will want to update this when I implement heartbeating between the
        # manager and the workers
        while socket.poll(3e5) > 0:
            msgb = socket.recv_multipart()

            cmd = msgb[1]
            match cmd:
                case b"sim":
                    try:
                        gen: int = struct.unpack("<I", msgb[2])
                        if gen != current_gen:
                            best_agent_fitness = 0.0
                            best_agent_data = None
                            current_gen = gen

                        agent = json.loads(msgb[4])
                        fitness, data = sim_fn(agent)
                        if fitness > best_agent_fitness:
                            best_agent_fitness = fitness
                            best_agent_data = data
                        fitness_b = struct.pack("<d", fitness)
                        socket.send_multipart(
                            [
                                exp_id_b,
                                b"done",
                                msgb[2],
                                msgb[3],
                                fitness_b,
                            ]
                        )
                        socket.send_multipart([exp_id_b, b"ready"])
                    except Exception as e:
                        socket.send_multipart(
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

                        data_b = json.dumps(best_agent_data).encode()
                        socket.send_multipart(
                            [exp_id_b, b"moredata", msgb[2], msgb[3], data_b]
                        )
                    except Exception as e:
                        socket.send_multipart(
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
    finally:
        socket.close()
        ctx.term()
