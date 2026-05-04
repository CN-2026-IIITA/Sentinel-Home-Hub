"""
Demo Async Client for the Pub/Sub Broker
==========================================
Provides three modes of operation:

    python -m client.client subscribe --topic sensors
    python -m client.client publish   --topic sensors --priority 200 --message "temp=42"
    python -m client.client time-travel --topic sensors --offset 0
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import struct
import sys

# Adjust import path so the script works when run as ``python -m client.client``
# from the project root.
sys.path.insert(0, ".")

from broker.protocol import (
    CMD_PUBLISH,
    CMD_SUBSCRIBE,
    CMD_TIME_TRAVEL,
    HEADER_SIZE,
    Message,
    decode_header,
    encode,
    topic_hash,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("client")


# ═════════════════════════════════════════════════════════════════
# Helpers
# ═════════════════════════════════════════════════════════════════

async def _read_one(reader: asyncio.StreamReader) -> Message | None:
    """Read a single message frame from the stream."""
    try:
        header = await reader.readexactly(HEADER_SIZE)
    except asyncio.IncompleteReadError:
        return None
    cmd, tid, pri, plen = decode_header(header)
    payload = b""
    if plen > 0:
        payload = await reader.readexactly(plen)
    return Message(command=cmd, topic_id=tid, priority=pri, payload=payload)


async def _listen(reader: asyncio.StreamReader) -> None:
    """Continuously read and print incoming messages."""
    while True:
        msg = await _read_one(reader)
        if msg is None:
            log.info("Server closed the connection")
            break
        text = msg.payload.decode(errors="replace")
        log.info("◀ RECEIVED  topic=%d  pri=%d  payload=%r", msg.topic_id, msg.priority, text)


# ═════════════════════════════════════════════════════════════════
# Modes
# ═════════════════════════════════════════════════════════════════

async def mode_subscribe(host: str, port: int, topic: str) -> None:
    """Subscribe to a topic and listen for incoming messages."""
    tid = topic_hash(topic)
    reader, writer = await asyncio.open_connection(host, port)
    log.info("Connected — subscribing to '%s' (topic_id=%d)", topic, tid)

    frame = encode(CMD_SUBSCRIBE, tid, 0)
    writer.write(frame)
    await writer.drain()

    await _listen(reader)
    writer.close()
    await writer.wait_closed()



# ═════════════════════════════════════════════════════════════════
# CLI
# ═════════════════════════════════════════════════════════════════

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Pub/Sub Broker Client")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9999)
    sub = p.add_subparsers(dest="mode", required=True)

    # subscribe
    s = sub.add_parser("subscribe", help="Subscribe to a topic")
    s.add_argument("--topic", required=True)

   

    return p


def main() -> None:
    args = build_parser().parse_args()
    try:
        if args.mode == "subscribe":
            asyncio.run(mode_subscribe(args.host, args.port, args.topic))
        
    except KeyboardInterrupt:
        log.info("Interrupted")


if __name__ == "__main__":
    main()
