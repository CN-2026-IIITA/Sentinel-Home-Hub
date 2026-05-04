"""
Smart Home Hub — Dashboard Server
==================================
Bridges the browser visualization to the Pub/Sub broker.

- Maintains a persistent TCP connection to the broker
- Subscribes to all Smart Home topics
- Fans out messages to browser clients via Server-Sent Events (SSE)
- Provides REST API for interactive device simulation

Usage::
    python dashboard_server.py
"""

import asyncio
import json
import logging
import os
import struct
import sys
import time
from urllib.parse import urlparse, parse_qs

# Import protocol utilities from our broker
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

# Global list of active SSE client queues
sse_clients = []
broker_connected = False

# ── Smart Home Topics ────────────────────────────────────────────
SMART_HOME_TOPICS = {
    topic_hash("home/fire"): "home/fire",
    topic_hash("home/temperature"): "home/temperature",
    topic_hash("home/door"): "home/door",
    topic_hash("home/light"): "home/light",
    topic_hash("home/battery"): "home/battery",
    topic_hash("broker/metrics"): "broker/metrics",
}

# Device simulation configs
DEVICE_CONFIGS = {
    "fire": {"topic": "home/fire", "priority": 255, "payload": "🔥 FIRE DETECTED in sector 7G!"},
    "temperature": {"topic": "home/temperature", "priority": 50, "payload": "temp=%.1f°C"},
    "door": {"topic": "home/door", "priority": 128, "payload": "door=OPEN location=front"},
    "light": {"topic": "home/light", "priority": 30, "payload": "light=ON room=living"},
    "battery": {"topic": "home/battery", "priority": 0, "payload": "battery=80%% device=sensor_01"},
}

import random
#1
async def _read_one(reader: asyncio.StreamReader):
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
    """Maintains a persistent connection to the broker and fans out to SSE clients."""
    global broker_connected
    while True:
        try:
            reader, writer = await asyncio.open_connection(BROKER_HOST, BROKER_PORT)
            log.info("Proxy connected to broker.")
            broker_connected = True
            await broadcast_sse({"type": "connection_status", "connected": True})

            # Subscribe to all Smart Home topics
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

                if tid in SMART_HOME_TOPICS:
                    topic_name = SMART_HOME_TOPICS[tid]
                    raw_hex = (header_bytes + payload).hex()

                    if topic_name == "broker/metrics":
                        try:
                            data = json.loads(payload.decode('utf-8'))
                            data["type"] = "metrics"
                            data["hex"] = raw_hex
                            await broadcast_sse(data)
                        except Exception as e:
                            log.error(f"Failed to parse metrics: {e}")
                    else:
                        data = {
                            "type": "qos",
                            "topic": topic_name,
                            "priority": pri,
                            "payload": payload.decode(errors='replace'),
                            "hex": raw_hex,
                            "timestamp": time.time(),
                        }
                        await broadcast_sse(data)

                await asyncio.sleep(0)
        except Exception as e:
            log.error(f"Broker connection error: {e}")

        broker_connected = False
        await broadcast_sse({"type": "connection_status", "connected": False})
        log.info("Reconnecting to broker in 2s...")
        await asyncio.sleep(2)


async def broadcast_sse(data: dict):
    if not sse_clients:
        return
    msg = f"data: {json.dumps(data)}\n\n"
    for q in sse_clients:
        # If queue is full, drop the oldest message to make room
        if q.full():
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                pass
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            pass  # should not happen after the drain above



# ── HTTP Server ──────────────────────────────────────────────────

async def handle_http_request(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    request_line = await reader.readline()
    if not request_line:
        writer.close()
        return

    req = request_line.decode().strip()
    parts = req.split(' ')
    if len(parts) < 2:
        writer.close()
        return
    method, path = parts[0], parts[1]
    log.info(f"HTTP {method} {path}")

    # Parse headers
    content_length = 0
    while True:
        line = await reader.readline()
        if line == b'\r\n' or not line:
            break
        header = line.decode().strip().lower()
        if header.startswith('content-length:'):
            content_length = int(header.split(':')[1].strip())

    parsed_path = urlparse(path)
    url_path = parsed_path.path

    # ── SSE Stream ───────────────────────────────────────────
    if method == "GET" and url_path == "/stream":
        writer.write(b"HTTP/1.1 200 OK\r\n")
        writer.write(b"Content-Type: text/event-stream\r\n")
        writer.write(b"Cache-Control: no-cache\r\n")
        writer.write(b"Connection: keep-alive\r\n")
        writer.write(b"Access-Control-Allow-Origin: *\r\n")
        writer.write(b"\r\n")
        await writer.drain()

        q = asyncio.Queue(maxsize=2000)
        sse_clients.append(q)
        log.info("SSE client connected [total: %d]", len(sse_clients))

        initial = f"data: {json.dumps({'type': 'connection_status', 'connected': broker_connected})}\n\n"
        writer.write(initial.encode('utf-8'))
        await writer.drain()

        try:
            while True:
                msg = await q.get()
                writer.write(msg.encode('utf-8'))
                await writer.drain()
        except (ConnectionResetError, ConnectionAbortedError,
                BrokenPipeError, OSError, Exception) as e:
            log.debug("SSE client error: %s", e)
        finally:
            if q in sse_clients:
                sse_clients.remove(q)
            log.info("SSE client disconnected [total: %d]", len(sse_clients))
            if not writer.is_closing():
                writer.close()
        return

    
    # ── CORS Preflight ───────────────────────────────────────
    elif method == "OPTIONS":
        writer.write(b"HTTP/1.1 204 No Content\r\n")
        writer.write(b"Access-Control-Allow-Origin: *\r\n")
        writer.write(b"Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n")
        writer.write(b"Access-Control-Allow-Headers: Content-Type\r\n")
        writer.write(b"\r\n")
        await writer.drain()
        writer.close()
        return

    # ── Serve Static Files ───────────────────────────────────
    if url_path == "/":
        url_path = "/index.html"
    filepath = os.path.normpath(os.path.join(STATIC_DIR, url_path.lstrip('/')))

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
        with open(filepath, 'rb') as f:
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
    if not os.path.exists(STATIC_DIR):
        os.makedirs(STATIC_DIR)

    asyncio.create_task(proxy_broker_connection())

    server = await asyncio.start_server(handle_http_request, "0.0.0.0", WEB_PORT)
    log.info(f"Dashboard Server listening on http://127.0.0.1:{WEB_PORT}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutting down Dashboard Server")
