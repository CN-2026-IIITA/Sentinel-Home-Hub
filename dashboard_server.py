"""
Smart Home Hub — Dashboard Server
=================================
HTTP + SSE proxy for the Pub/Sub broker.

Purpose
-------
- Keeps one TCP connection to the broker.
- Subscribes to all smart-home topics.
- Streams live events to browsers via Server-Sent Events (SSE).
- Exposes REST endpoints to simulate devices and trigger time-travel.
"""

# Acts as an HTTP proxy/bridge. It connects to the TCP broker as a client,
# translates the binary data into Server-Sent Events (SSE), and serves the dashboard to a web browser.
# It decouples the heavy web/HTTP processing from the lightning-fast TCP broker.
# It allows browsers (which don't speak raw TCP binary) to visualize the system in real-time.

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import struct
import sys
import time
from urllib.parse import urlparse

# Import protocol utilities from broker
sys.path.insert(0, ".")
from broker.protocol import (
    CMD_PUBLISH,
    CMD_SUBSCRIBE,
    CMD_TIME_TRAVEL,
    HEADER_SIZE,
    encode,
    decode_header,
    topic_hash,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("dashboard_server")

BROKER_HOST = "127.0.0.1"
BROKER_PORT = 9999
WEB_PORT = 8080
STATIC_DIR = os.path.join(os.path.dirname(__file__), "visualization")

sse_clients: list[asyncio.Queue[str]] = []
broker_connected = False

SMART_HOME_TOPICS = {
    topic_hash("home/fire"): "home/fire",
    topic_hash("home/temperature"): "home/temperature",
    topic_hash("home/door"): "home/door",
    topic_hash("home/light"): "home/light",
    topic_hash("home/battery"): "home/battery",
    topic_hash("broker/metrics"): "broker/metrics",
}

DEVICE_CONFIGS = {
    "fire": {"topic": "home/fire", "priority": 255, "payload": "🔥 FIRE DETECTED in sector 7G!"},
    "temperature": {"topic": "home/temperature", "priority": 50, "payload": "temp=%.1f°C"},
    "door": {"topic": "home/door", "priority": 128, "payload": "door=OPEN location=front"},
    "light": {"topic": "home/light", "priority": 30, "payload": "light=ON room=living"},
    "battery": {"topic": "home/battery", "priority": 0, "payload": "battery=80%% device=sensor_01"},
}

async def _read_one(reader: asyncio.StreamReader):
    try:
        header = await reader.readexactly(HEADER_SIZE)
    except:
        return None

    cmd, tid, pri, plen = decode_header(header)
    payload = b""

    if plen > 0:
        try:
            payload = await reader.readexactly(plen)
        except:
            return None

    return cmd, tid, pri, payload, header

async def proxy_broker_connection():
    global broker_connected

    backoff = 1
    while True:
        try:
            reader, writer = await asyncio.open_connection(BROKER_HOST, BROKER_PORT)
            broker_connected = True
            await broadcast_sse({"type": "connection_status", "connected": True})

            for tid in SMART_HOME_TOPICS:
                writer.write(encode(CMD_SUBSCRIBE, tid, 0))
            await writer.drain()

            while True:
                msg = await _read_one(reader)
                if msg is None:
                    break

                cmd, tid, pri, payload, header_bytes = msg
                topic_name = SMART_HOME_TOPICS.get(tid)
                if not topic_name:
                    continue

                raw_hex = (header_bytes + payload).hex()

                data = {
                    "type": "qos",
                    "topic": topic_name,
                    "priority": pri,
                    "payload": payload.decode(errors="replace"),
                    "hex": raw_hex,
                    "timestamp": time.time(),
                }
                await broadcast_sse(data)

        except Exception:
            pass

        broker_connected = False
        await broadcast_sse({"type": "connection_status", "connected": False})
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 10)

async def broadcast_sse(data: dict):
    if not sse_clients:
        return

    msg = f"data: {json.dumps(data)}\n\n"
    for q in sse_clients:
        if not q.full():
            q.put_nowait(msg)

async def simulate_device(device: str):
    config = DEVICE_CONFIGS.get(device)
    if not config:
        return False

    try:
        reader, writer = await asyncio.open_connection(BROKER_HOST, BROKER_PORT)
        tid = topic_hash(config["topic"])

        payload_str = config["payload"]
        if device == "temperature":
            payload_str = config["payload"] % (20 + random.random() * 25)

        frame = encode(CMD_PUBLISH, tid, config["priority"], payload_str.encode())
        writer.write(frame)
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        return True
    except:
        return False

async def handle_http_request(reader, writer):
    request_line = await reader.readline()
    if not request_line:
        writer.close()
        return

    req = request_line.decode().strip()
    method, path = req.split(" ")[:2]

    if method == "GET" and path == "/stream":
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\n")
        await writer.drain()

        q = asyncio.Queue()
        sse_clients.append(q)

        try:
            while True:
                msg = await q.get()
                writer.write(msg.encode())
                await writer.drain()
        except:
            pass

async def main():
    asyncio.create_task(proxy_broker_connection())
    server = await asyncio.start_server(handle_http_request, "0.0.0.0", WEB_PORT)
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())