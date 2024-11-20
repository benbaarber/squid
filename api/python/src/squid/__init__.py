import json
import os
from typing import Callable
import zmq


def run(sim_fn: Callable[[dict], dict]):
    """
    Run the simulation loop.

    :param sim_fn: A callable that takes an agent (dict) and returns a result (dict).
    """
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
        socket.send(b"ready")

        # TODO: sub to broker, poll for a heartbeat, more reliable
        while socket.poll(3e5) > 0:
            msgb = socket.recv_multipart()
            cmd = msgb[0]
            if cmd == b"sim":
                agent = json.loads(msgb[2])
                try:
                    result = sim_fn(agent)
                    socket.send_multipart(
                        [b"done", msgb[1], json.dumps(result).encode("utf-8")]
                    )
                    socket.send(b"ready")
                except Exception as e:
                    print("Simulation error:", e)
                    socket.send_multipart([b"error", msgb[1], str(e).encode("utf-8")])
            elif cmd == b"kill":
                break
            else:
                print("Received invalid command:", cmd)
    finally:
        socket.close()
        ctx.term()
