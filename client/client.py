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
    """Subscribe to a topic (or multiple comma-separated topics) and listen."""
    reader, writer = await asyncio.open_connection(host, port)
    
    topics = [t.strip() for t in topic.split(",") if t.strip()]
    for t in topics:
        tid = topic_hash(t)
        log.info("Connected — subscribing to '%s' (topic_id=%d)", t, tid)
        frame = encode(CMD_SUBSCRIBE, tid, 0)
        writer.write(frame)
        
    await writer.drain()
    await _listen(reader)
    writer.close()
    await writer.wait_closed()


async def mode_publish(host: str, port: int, topic: str, priority: int, message: str) -> None:
    """Publish a single message and disconnect."""
    tid = topic_hash(topic)
    reader, writer = await asyncio.open_connection(host, port)
    log.info("Connected — publishing to '%s' (topic_id=%d, pri=%d)", topic, tid, priority)

    frame = encode(CMD_PUBLISH, tid, priority, message.encode())
    writer.write(frame)
    await writer.drain()

    log.info("Message sent ✔")
    writer.close()
    await writer.wait_closed()


async def mode_publish_interactive(host: str, port: int, topic: str, priority: int) -> None:
    """Interactively publish messages from stdin."""
    tid = topic_hash(topic)
    reader, writer = await asyncio.open_connection(host, port)
    log.info("Connected — interactive publish mode on '%s' (topic_id=%d)", topic, tid)
    log.info("Type messages and press Enter.  Ctrl+C to quit.\n")

    try:
        loop = asyncio.get_running_loop()
        while True:
            line = await loop.run_in_executor(None, sys.stdin.readline)
            if not line:
                break
            text = line.strip()
            if not text:
                continue
            frame = encode(CMD_PUBLISH, tid, priority, text.encode())
            writer.write(frame)
            await writer.drain()
            log.info("Sent ✔  %r", text)
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        writer.close()
        await writer.wait_closed()


async def mode_time_travel(host: str, port: int, topic: str, offset: int) -> None:
    """
    Send a time-travel subscribe command, receive historical
    messages, then listen for real-time messages.
    """
    tid = topic_hash(topic)
    reader, writer = await asyncio.open_connection(host, port)
    log.info("Connected — time-travel on '%s' from offset %d", topic, offset)

    payload = struct.pack("!Q", offset)
    frame = encode(CMD_TIME_TRAVEL, tid, 0, payload)
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

    # publish (single message)
    s = sub.add_parser("publish", help="Publish a single message")
    s.add_argument("--topic", required=True)
    s.add_argument("--priority", type=int, default=128)
    s.add_argument("--message", required=True)

    # publish-interactive
    s = sub.add_parser("publish-interactive", help="Publish messages interactively from stdin")
    s.add_argument("--topic", required=True)
    s.add_argument("--priority", type=int, default=128)

    # time-travel
    s = sub.add_parser("time-travel", help="Replay from a log offset, then go live")
    s.add_argument("--topic", required=True)
    s.add_argument("--offset", type=int, default=0)

    return p


def main() -> None:
    args = build_parser().parse_args()
    try:
        if args.mode == "subscribe":
            asyncio.run(mode_subscribe(args.host, args.port, args.topic))
        elif args.mode == "publish":
            asyncio.run(mode_publish(args.host, args.port, args.topic, args.priority, args.message))
        elif args.mode == "publish-interactive":
            asyncio.run(mode_publish_interactive(args.host, args.port, args.topic, args.priority))
        elif args.mode == "time-travel":
            asyncio.run(mode_time_travel(args.host, args.port, args.topic, args.offset))
    except KeyboardInterrupt:
        log.info("Interrupted")


if __name__ == "__main__":
    main()
