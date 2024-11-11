import json
import os
from typing import Callable
import zmq


def run(sim_fn: Callable[[dict], dict]):
    """
    Run the simulation client loop.

    :param sim_fn: A callable that takes an agent (dict) and returns a result (dict).
    """
    broker_addr = os.environ.get("SQUID_BROKER_ADDR")
    if broker_addr is None:
        raise EnvironmentError("Environment variable 'SQUID_BROKER_ADDR' is undefined.")

    if not callable(sim_fn):
        raise ValueError(
            "The sim_fn argument must be a callable that accepts a dict and returns a dict."
        )

    ctx = zmq.Context()
    socket = ctx.socket(zmq.DEALER)

    try:
        socket.connect(broker_addr)
        socket.send(b"ready")

        while True:
            msgb = socket.recv_multipart()
            cmd = msgb[0].decode("utf-8")
            if cmd == "sim":
                agent = json.loads(msgb[2])
                result = sim_fn(agent)
                socket.send_multipart(
                    [b"done", msgb[1], json.dumps(result).encode("utf-8")]
                )
            elif cmd == "kill":
                break
            else:
                print("Received invalid command:" + cmd)
    finally:
        socket.close()
        ctx.term()
