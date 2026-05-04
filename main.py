"""
Broker Entry Point
==================
Start the Pub/Sub broker from the command line.

Usage::

    python main.py
    python main.py --host 0.0.0.0 --port 8888
    python main.py --log broker_data.bin
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys

from broker.server import BrokerServer

# ── Logging ───────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(name)-22s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("main")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Async Pub/Sub Message Broker")
    p.add_argument("--host", default="127.0.0.1", help="Bind address (default: 127.0.0.1)")
    p.add_argument("--port", type=int, default=9999, help="Bind port (default: 9999)")
    p.add_argument("--log", default="broker_log.bin", help="Binary log file path")
    return p.parse_args()


async def run(args: argparse.Namespace) -> None:
    server = BrokerServer(host=args.host, port=args.port, log_path=args.log)

    # Graceful shutdown on Ctrl+C / SIGTERM
    loop = asyncio.get_running_loop()

    def _signal_handler() -> None:
        log.info("Received shutdown signal")
        asyncio.create_task(server.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            # Windows does not support add_signal_handler for SIGTERM
            pass

    log.info("Starting broker on %s:%d  (log: %s)", args.host, args.port, args.log)

    try:
        await server.start()
    except asyncio.CancelledError:
        pass
    finally:
        await server.stop()
        log.info("Broker stopped cleanly")


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        log.info("Interrupted — exiting")
        sys.exit(0)


if __name__ == "__main__":
    main()
