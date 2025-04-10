from multiprocessing import Process
import os
import struct
from typing import Callable

# (genome) -> fitness, data
SimFn = Callable[[str], tuple[float, dict | None]]

# (exp_id_b, wk_router_url, wk_ids) -> workers
WorkerFactory = Callable[[bytes, str, list[int]], dict[int, Process]]


def de_i32(bytes: bytes) -> int:
    return struct.unpack("!i", bytes)[0]


def se_i32(x: int) -> bytes:
    return struct.pack("!i", x)


def de_f64(bytes: bytes) -> int:
    return struct.unpack("!d", bytes)[0]


def se_f64(x: float) -> bytes:
    return struct.pack("!d", x)


def de_u32(bytes: bytes) -> int:
    return struct.unpack("!I", bytes)[0]


def se_u32(x: int) -> bytes:
    return struct.pack("!I", x)


def de_u64(bytes: bytes) -> int:
    return struct.unpack("!Q", bytes)[0]


def se_u64(x: int) -> bytes:
    return struct.pack("!Q", x)


def get_env_or_throw(key: str) -> str:
    val = os.environ.get(key)
    if val is None:
        raise EnvironmentError(f"Environment variable {key} not set")
    return val
