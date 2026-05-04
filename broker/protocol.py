"""
Phase 2: Custom Binary Protocol (Codec Layer)
==============================================
All network messages are serialized into a compact binary format
using the ``struct`` module, avoiding JSON overhead entirely.

Packet Layout (10-byte fixed header + variable payload):
    Byte  0      : Command Type   (uint8)   — 0x01=Subscribe, 0x02=Publish, 0x03=TimeTravel
    Bytes 1-4    : Topic ID       (uint32)  — typically a hash of the topic string
    Byte  5      : Priority Level (uint8)   — 0-255, 255 = highest
    Bytes 6-9    : Payload Length  (uint32)
    Bytes 10+    : Payload        (raw bytes)
"""

from __future__ import annotations

import hashlib
import struct
from typing import NamedTuple

# ── Command constants ────────────────────────────────────────────
CMD_SUBSCRIBE: int = 0x01
CMD_PUBLISH: int = 0x02
CMD_TIME_TRAVEL: int = 0x03

# ── Header layout ────────────────────────────────────────────────
# '!' = network byte-order (big-endian)
# 'B'  = uint8   (command)
# 'I'  = uint32  (topic_id)
# 'B'  = uint8   (priority)
# 'I'  = uint32  (payload_length)
HEADER_FORMAT: str = "!BIBI"
HEADER_SIZE: int = struct.calcsize(HEADER_FORMAT)  # 10 bytes


class Message(NamedTuple):
    """Decoded message container."""
    command: int
    topic_id: int
    priority: int
    payload: bytes


# ── Public helpers ────────────────────────────────────────────────

def topic_hash(topic: str) -> int:
    """
    Hash a human-readable topic string into a 32-bit unsigned integer.

    Uses the first 4 bytes of an MD5 digest for uniform distribution.
    Collisions are possible (~50 % at ~77 K topics) but acceptable
    for this broker's scope.
    """
    digest = hashlib.md5(topic.encode()).digest()
    return struct.unpack("!I", digest[:4])[0]


def encode(command: int, topic_id: int, priority: int, payload: bytes = b"") -> bytes:
    """
    Pack a full message frame (header + payload) into bytes.

    Parameters
    ----------
    command : int
        One of CMD_SUBSCRIBE, CMD_PUBLISH, CMD_TIME_TRAVEL.
    topic_id : int
        32-bit unsigned topic identifier.
    priority : int
        0-255 priority level (255 = highest).
    payload : bytes
        Raw payload data.

    Returns
    -------
    bytes
        The complete wire-format frame.
    """
    header = struct.pack(HEADER_FORMAT, command, topic_id, priority, len(payload))
    return header + payload


def decode_header(data: bytes) -> tuple[int, int, int, int]:
    """
    Unpack a 10-byte header.

    Returns
    -------
    tuple[command, topic_id, priority, payload_length]
    """
    if len(data) < HEADER_SIZE:
        raise ValueError(f"Header too short: expected {HEADER_SIZE} bytes, got {len(data)}")
    return struct.unpack(HEADER_FORMAT, data[:HEADER_SIZE])


async def read_message(reader) -> Message | None:
    """
    Read one complete message from an ``asyncio.StreamReader``.

    Returns ``None`` when the remote end has closed the connection.
    """
    header_data = await reader.readexactly(HEADER_SIZE)
    if not header_data:
        return None

    command, topic_id, priority, payload_len = decode_header(header_data)

    payload = b""
    if payload_len > 0:
        payload = await reader.readexactly(payload_len)

    return Message(command=command, topic_id=topic_id, priority=priority, payload=payload)
