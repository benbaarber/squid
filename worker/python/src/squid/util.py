import struct
from typing import Callable

SimFn = Callable[[dict], tuple[float, dict | None]]


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
