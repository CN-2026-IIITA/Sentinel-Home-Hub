"""
Web Dashboard Bridge
=====================
Connects to the Pub/Sub broker as a subscriber AND runs a lightweight
HTTP server to serve a real-time web dashboard.

Uses **Server-Sent Events (SSE)** to push broker messages to the
browser — zero third-party dependencies.

Usage::
    python client/dashboard.py
    python client/dashboard.py --broker-port 9999 --http-port 8080 --topics sensors,alerts
"""

import argparse
import asyncio
import json
import logging
import os
import urllib.parse
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from broker.protocol import (
    CMD_PUBLISH,
    CMD_SUBSCRIBE,
    HEADER_SIZE,
    decode_header,
    encode,
    topic_hash,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("dashboard")

# Global config passed from CLI
CONFIG = {"broker_host": "127.0.0.1", "broker_port": 9999}

# ═════════════════════════════════════════════════════════════════
# HTTP Server
# ═════════════════════════════════════════════════════════════════

async def http_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Handle a single HTTP request."""
    try:
        request_line = await asyncio.wait_for(reader.readline(), timeout=5.0)
        if not request_line:
            writer.close()
            return

        request_str = request_line.decode(errors="replace").strip()
        parts = request_str.split(" ")
        if len(parts) < 2:
            writer.close()
            return

        method, full_path = parts[0], parts[1]
        
        # Parse query params
        parsed_url = urllib.parse.urlparse(full_path)
        path = parsed_url.path
        query = urllib.parse.parse_qs(parsed_url.query)

        # Read all headers
        headers: dict[str, str] = {}
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=5.0)
            decoded = line.decode(errors="replace").strip()
            if not decoded:
                break
            if ":" in decoded:
                key, val = decoded.split(":", 1)
                headers[key.strip().lower()] = val.strip()

        # Route
        if method == "GET" and path == "/":
            await serve_html(writer)
        
        else:
            await send_response(writer, 404, "text/plain", b"Not Found")

    except (asyncio.TimeoutError, ConnectionResetError, BrokenPipeError, OSError):
        pass
    finally:
        if not writer.is_closing():
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

async def serve_html(writer: asyncio.StreamWriter) -> None:
    html_path = os.path.join(os.path.dirname(__file__), "index.html")
    with open(html_path, "rb") as f:
        content = f.read()
    await send_response(writer, 200, "text/html; charset=utf-8", content)


async def send_response(writer: asyncio.StreamWriter, status: int, content_type: str, body: bytes) -> None:
    status_text = {200: "OK", 400: "Bad Request", 404: "Not Found"}
    header = (
        f"HTTP/1.1 {status} {status_text.get(status, 'Unknown')}\r\n"
        f"Content-Type: {content_type}\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"Connection: close\r\n"
        f"Access-Control-Allow-Origin: *\r\n\r\n"
    )
    writer.write(header.encode() + body)
    await writer.drain()

async def run(args: argparse.Namespace) -> None:
    CONFIG["broker_host"] = args.broker_host
    CONFIG["broker_port"] = args.broker_port

    http_server = await asyncio.start_server(http_handler, args.http_host, args.http_port)
    log.info("Dashboard HTTP server on http://%s:%d", args.http_host, args.http_port)

    async with http_server:
        await http_server.serve_forever()

def main() -> None:
    p = argparse.ArgumentParser(description="Pub/Sub Web Dashboard")
    p.add_argument("--broker-host", default="127.0.0.1")
    p.add_argument("--broker-port", type=int, default=9999)
    p.add_argument("--http-host", default="127.0.0.1")
    p.add_argument("--http-port", type=int, default=8080)
    args = p.parse_args()

    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        log.info("Dashboard stopped")

if __name__ == "__main__":
    main()
