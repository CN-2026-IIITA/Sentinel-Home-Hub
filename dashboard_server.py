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
from broker.protocol import (  # noqa: E402
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

# Connected SSE clients (each client has a queue)
sse_clients: list[asyncio.Queue[str]] = []
broker_connected = False

# Known topics -> human readable names
SMART_HOME_TOPICS = {
    topic_hash("home/fire"): "home/fire",
    topic_hash("home/temperature"): "home/temperature",
    topic_hash("home/door"): "home/door",
    topic_hash("home/light"): "home/light",
    topic_hash("home/battery"): "home/battery",
    topic_hash("broker/metrics"): "broker/metrics",
}

# Device simulation templates
DEVICE_CONFIGS = {
    "fire": {"topic": "home/fire", "priority": 255, "payload": "🔥 FIRE DETECTED in sector 7G!"},
    "temperature": {"topic": "home/temperature", "priority": 50, "payload": "temp=%.1f°C"},
    "door": {"topic": "home/door", "priority": 128, "payload": "door=OPEN location=front"},
    "light": {"topic": "home/light", "priority": 30, "payload": "light=ON room=living"},
    "battery": {"topic": "home/battery", "priority": 0, "payload": "battery=80%% device=sensor_01"},
}


# ────────────────────────────────────────────────────────────────
# Broker <-> Proxy Bridge
# ────────────────────────────────────────────────────────────────

async def _read_one(reader: asyncio.StreamReader):
    """
    Read exactly one broker frame.

    Role: Ensures clean frame boundaries (prevents TCP packet sticking).
    Returns: (cmd, topic_id, priority, payload, header_bytes) or None on disconnect.
    """
    try:
        header = await reader.readexactly(HEADER_SIZE)
    except (asyncio.IncompleteReadError, ConnectionResetError, OSError):
        return None

    cmd, tid, pri, plen = decode_header(header)
    payload = b""

    if plen > 0:
        try:
            payload = await reader.readexactly(plen)
        except (asyncio.IncompleteReadError, ConnectionResetError, OSError):
            return None

    return cmd, tid, pri, payload, header


async def proxy_broker_connection():
    """
    Persistent connection to broker.

    Role: Subscribes to topics and streams messages to SSE clients.
    Importance: Acts as the live bridge between TCP broker and browser.
    """
    global broker_connected

    backoff = 1
    max_backoff = 10

    while True:
        try:
            reader, writer = await asyncio.open_connection(BROKER_HOST, BROKER_PORT)
            broker_connected = True
            backoff = 1
            log.info("Proxy connected to broker.")
            await broadcast_sse({"type": "connection_status", "connected": True})

            # Subscribe to all known topics
            for tid in SMART_HOME_TOPICS:
                writer.write(encode(CMD_SUBSCRIBE, tid, 0))
            await writer.drain()
            log.info("Subscribed to %d topics.", len(SMART_HOME_TOPICS))

            while True:
                msg = await _read_one(reader)
                if msg is None:
                    log.warning("Broker closed the connection.")
                    break

                cmd, tid, pri, payload, header_bytes = msg
                topic_name = SMART_HOME_TOPICS.get(tid)
                if not topic_name:
                    continue

                raw_hex = (header_bytes + payload).hex()

                if topic_name == "broker/metrics":
                    try:
                        data = json.loads(payload.decode("utf-8"))
                        data["type"] = "metrics"
                        data["hex"] = raw_hex
                        await broadcast_sse(data)
                    except Exception as e:
                        log.error("Failed to parse metrics: %s", e)
                else:
                    data = {
                        "type": "qos",
                        "topic": topic_name,
                        "priority": pri,
                        "payload": payload.decode(errors="replace"),
                        "hex": raw_hex,
                        "timestamp": time.time(),
                    }
                    await broadcast_sse(data)

        except Exception as e:
            log.error("Broker connection error: %s", e)

        broker_connected = False
        await broadcast_sse({"type": "connection_status", "connected": False})
        log.info("Reconnecting in %ds...", backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, max_backoff)


async def broadcast_sse(data: dict):
    """
    Push an event to all connected SSE clients.

    Role: Converts dict -> JSON -> SSE format.
    Importance: Keeps UI real-time and non-blocking.
    """
    if not sse_clients:
        return

    msg = f"data: {json.dumps(data)}\n\n"
    for q in sse_clients:
        if q.full():
            try:
                q.get_nowait()  # drop oldest if slow client
            except asyncio.QueueEmpty:
                pass
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            pass


# ────────────────────────────────────────────────────────────────
# Simulation Helpers
# ────────────────────────────────────────────────────────────────

async def simulate_device(device: str):
    """
    Simulate a single device publish.

    Importance: Demonstrates message flow from UI -> broker -> UI.
    """
    config = DEVICE_CONFIGS.get(device)
    if not config:
        return False

    try:
        reader, writer = await asyncio.open_connection(BROKER_HOST, BROKER_PORT)
        tid = topic_hash(config["topic"])

        payload_str = config["payload"]
        if device == "temperature":
            payload_str = config["payload"] % (20.0 + random.random() * 25.0)

        frame = encode(CMD_PUBLISH, tid, config["priority"], payload_str.encode())
        writer.write(frame)
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        log.info("Simulated %s: %s", device, payload_str)
        return True
    except Exception as e:
        log.error("Simulate %s error: %s", device, e)
        return False


async def simulate_burst():
    """
    Send a mixed high/low-priority burst over a single TCP connection.

    Importance: Visual proof of QoS ordering.
    """
    try:
        reader, writer = await asyncio.open_connection(BROKER_HOST, BROKER_PORT)

        messages = []
        for _ in range(5):
            cfg = DEVICE_CONFIGS["fire"]
            messages.append((cfg["topic"], cfg["priority"], cfg["payload"]))
        for _ in range(20):
            cfg = DEVICE_CONFIGS["battery"]
            messages.append((cfg["topic"], cfg["priority"], cfg["payload"]))
        for _ in range(5):
            cfg = DEVICE_CONFIGS["fire"]
            messages.append((cfg["topic"], cfg["priority"], cfg["payload"]))
        for _ in range(20):
            cfg = DEVICE_CONFIGS["battery"]
            messages.append((cfg["topic"], cfg["priority"], cfg["payload"]))

        for topic_str, pri, payload_str in messages:
            tid = topic_hash(topic_str)
            frame = encode(CMD_PUBLISH, tid, pri, payload_str.encode())
            writer.write(frame)

        await writer.drain()
        writer.close()
        await writer.wait_closed()
        log.info("Burst sent %d messages over single connection", len(messages))
        return True
    except Exception as e:
        log.error("Burst error: %s", e)
        return False


async def trigger_time_travel(offset: int):
    """
    Request replay from a byte offset and stream events to SSE.

    Importance: Demonstrates event-sourcing + time-travel capability.
    """
    if offset < 0:
        await broadcast_sse({"type": "time_travel_done", "replayed": 0})
        return

    writer = None
    try:
        reader, writer = await asyncio.open_connection(BROKER_HOST, BROKER_PORT)
        tid = topic_hash("home/fire")
        payload = struct.pack("!Q", offset)
        writer.write(encode(CMD_TIME_TRAVEL, tid, 0, payload))
        await writer.drain()

        metrics_tid = topic_hash("broker/metrics")
        replayed = 0
        max_replay = 500

        while replayed < max_replay:
            try:
                msg = await asyncio.wait_for(_read_one(reader), timeout=2.0)
                if msg is None:
                    break

                cmd, t, pri, p, hdr = msg
                if t == metrics_tid:
                    continue

                topic_name = SMART_HOME_TOPICS.get(t)
                if not topic_name:
                    continue

                data = {
                    "type": "time_travel_log",
                    "topic": topic_name,
                    "priority": pri,
                    "payload": p.decode(errors="replace"),
                    "hex": (hdr + p).hex(),
                    "timestamp": time.time(),
                    "history": True,
                }
                await broadcast_sse(data)
                replayed += 1

                if replayed % 10 == 0:
                    await asyncio.sleep(0.05)

            except (asyncio.TimeoutError, asyncio.CancelledError):
                break
            except Exception as e:
                log.warning("Time travel read error: %s", e)
                break

        await broadcast_sse({"type": "time_travel_done", "replayed": replayed})
        log.info("Time travel completed: replayed %d from offset %d", replayed, offset)

    except Exception as e:
        log.error("Time travel error: %s", e)
    finally:
        if writer and not writer.is_closing():
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass


# ────────────────────────────────────────────────────────────────
# HTTP Server
# ────────────────────────────────────────────────────────────────

async def handle_http_request(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """
    Minimal HTTP handler for:
    - /stream (SSE)
    - /api/simulate
    - /api/time-travel
    - static dashboard files
    """
    request_line = await reader.readline()
    if not request_line:
        writer.close()
        return

    req = request_line.decode().strip()
    parts = req.split(" ")
    if len(parts) < 2:
        writer.close()
        return

    method, path = parts[0], parts[1]
    log.info("HTTP %s %s", method, path)

    # Parse headers
    content_length = 0
    while True:
        line = await reader.readline()
        if line == b"\r\n" or not line:
            break
        header = line.decode().strip().lower()
        if header.startswith("content-length:"):
            content_length = int(header.split(":")[1].strip())

    parsed_path = urlparse(path)
    url_path = parsed_path.path

    # SSE Stream
    if method == "GET" and url_path == "/stream":
        writer.write(b"HTTP/1.1 200 OK\r\n")
        writer.write(b"Content-Type: text/event-stream\r\n")
        writer.write(b"Cache-Control: no-cache\r\n")
        writer.write(b"Connection: keep-alive\r\n")
        writer.write(b"Access-Control-Allow-Origin: *\r\n")
        writer.write(b"\r\n")
        await writer.drain()

        q: asyncio.Queue[str] = asyncio.Queue(maxsize=2000)
        sse_clients.append(q)
        log.info("SSE client connected [total: %d]", len(sse_clients))

        initial = f"data: {json.dumps({'type': 'connection_status', 'connected': broker_connected})}\n\n"
        writer.write(initial.encode("utf-8"))
        await writer.drain()

        try:
            while True:
                msg = await q.get()
                writer.write(msg.encode("utf-8"))
                await writer.drain()
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError, OSError) as e:
            log.debug("SSE client error: %s", e)
        finally:
            if q in sse_clients:
                sse_clients.remove(q)
            log.info("SSE client disconnected [total: %d]", len(sse_clients))
            if not writer.is_closing():
                writer.close()
        return

    # Simulate Device API
    if method == "POST" and url_path == "/api/simulate":
        body = await reader.readexactly(content_length) if content_length else b""
        try:
            data = json.loads(body.decode())
            device = data.get("device", "temperature")
            if device == "burst":
                task = asyncio.create_task(simulate_burst())
            else:
                task = asyncio.create_task(simulate_device(device))
            task.add_done_callback(lambda t: t.exception() if not t.cancelled() and t.exception() else None)

            response = b'{"status":"ok"}'
            writer.write(b"HTTP/1.1 200 OK\r\n")
            writer.write(b"Content-Type: application/json\r\n")
            writer.write(b"Access-Control-Allow-Origin: *\r\n")
            writer.write(f"Content-Length: {len(response)}\r\n\r\n".encode())
            writer.write(response)
        except Exception:
            writer.write(b"HTTP/1.1 400 Bad Request\r\n\r\n")
        await writer.drain()
        writer.close()
        return

    # Time Travel API
    if method == "POST" and url_path == "/api/time-travel":
        body = await reader.readexactly(content_length) if content_length else b""
        try:
            data = json.loads(body.decode())
            offset = int(data.get("offset", 0))
            task = asyncio.create_task(trigger_time_travel(offset))
            task.add_done_callback(lambda t: t.exception() if not t.cancelled() and t.exception() else None)

            response = b'{"status":"ok"}'
            writer.write(b"HTTP/1.1 200 OK\r\n")
            writer.write(b"Content-Type: application/json\r\n")
            writer.write(b"Access-Control-Allow-Origin: *\r\n")
            writer.write(f"Content-Length: {len(response)}\r\n\r\n".encode())
            writer.write(response)
        except Exception:
            writer.write(b"HTTP/1.1 400 Bad Request\r\n\r\n")
        await writer.drain()
        writer.close()
        return

    # CORS preflight
    if method == "OPTIONS":
        writer.write(b"HTTP/1.1 204 No Content\r\n")
        writer.write(b"Access-Control-Allow-Origin: *\r\n")
        writer.write(b"Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n")
        writer.write(b"Access-Control-Allow-Headers: Content-Type\r\n")
        writer.write(b"\r\n")
        await writer.drain()
        writer.close()
        return

    # Static files
    if url_path == "/":
        url_path = "/index.html"

    filepath = os.path.normpath(os.path.join(STATIC_DIR, url_path.lstrip("/")))
    if not filepath.startswith(STATIC_DIR) or not os.path.exists(filepath):
        writer.write(b"HTTP/1.1 404 Not Found\r\n\r\n")
        await writer.drain()
        writer.close()
        return

    ext = os.path.splitext(filepath)[1]
    content_types = {
        ".html": "text/html",
        ".css": "text/css",
        ".js": "application/javascript",
        ".png": "image/png",
        ".ico": "image/x-icon",
    }
    ctype = content_types.get(ext, "application/octet-stream")

    try:
        with open(filepath, "rb") as f:
            content = f.read()
        writer.write(b"HTTP/1.1 200 OK\r\n")
        writer.write(f"Content-Type: {ctype}\r\n".encode())
        writer.write(b"Cache-Control: no-store\r\n")
        writer.write(b"Connection: close\r\n")
        writer.write(f"Content-Length: {len(content)}\r\n\r\n".encode())
        writer.write(content)
        await writer.drain()
    except Exception:
        writer.write(b"HTTP/1.1 500 Internal Server Error\r\n\r\n")
        await writer.drain()
    finally:
        writer.close()


async def main():
    """
    Entry point.
    - Ensures static directory exists.
    - Starts the broker proxy task.
    - Starts the HTTP server for UI + SSE.
    """
    if not os.path.exists(STATIC_DIR):
        os.makedirs(STATIC_DIR)

    asyncio.create_task(proxy_broker_connection())

    server = await asyncio.start_server(handle_http_request, "0.0.0.0", WEB_PORT)
    log.info("Dashboard Server listening on http://127.0.0.1:%d", WEB_PORT)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutting down Dashboard Server")