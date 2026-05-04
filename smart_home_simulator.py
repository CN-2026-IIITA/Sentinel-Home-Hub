"""
Smart Home IoT Device Simulator
=================================
Simulates 4 IoT devices publishing to the Smart Home Hub broker.

Each device runs as an independent async task:
  🌡️  Temperature Sensor  — publishes every 2s,  priority 50
  🔥  Fire Alarm          — publishes status every 30s, priority 10 (can be triggered)
  🚪  Door Sensor         — random open/close every 5-15s, priority 128
  💡  Smart Light         — random on/off every 8-20s, priority 30

Usage::
    python smart_home_simulator.py
    python smart_home_simulator.py --host 127.0.0.1 --port 9999
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import random
import sys

sys.path.insert(0, ".")

from broker.protocol import CMD_PUBLISH, encode, topic_hash

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("simulator")


async def publish_one(host: str, port: int, topic: str, priority: int, payload: str) -> bool:
    """Open a quick TCP connection, publish one message, close."""
    try:
        _reader, writer = await asyncio.open_connection(host, port)
        tid = topic_hash(topic)
        frame = encode(CMD_PUBLISH, tid, priority, payload.encode())
        writer.write(frame)
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        return True
    except (ConnectionRefusedError, OSError) as e:
        log.warning("Broker unreachable: %s", e)
        return False


# ── Device Simulators ────────────────────────────────────────────

async def temperature_sensor(host: str, port: int) -> None:
    """🌡️ Publishes temperature readings every 2 seconds."""
    log.info("🌡️  Temperature Sensor started")
    base_temp = 22.0
    while True:
        # Simulate gradual temperature drift
        base_temp += random.uniform(-0.5, 0.6)
        base_temp = max(15.0, min(50.0, base_temp))
        payload = f"temp={base_temp:.1f}°C"
        ok = await publish_one(host, port, "home/temperature", 50, payload)
        if ok:
            log.info("🌡️  Published: %s", payload)
        await asyncio.sleep(2.0)


async def fire_alarm(host: str, port: int) -> None:
    """🔥 Publishes status OK every 30s. Randomly triggers a fire alert (1% chance per tick)."""
    log.info("🔥  Fire Alarm started")
    while True:
        # 2% chance of fire per 30-second cycle (for demo excitement)
        if random.random() < 0.02:
            payload = "🔥 FIRE DETECTED in sector 7G! Evacuate immediately!"
            ok = await publish_one(host, port, "home/fire", 255, payload)
            if ok:
                log.info("🔥  ⚠️ FIRE ALARM TRIGGERED: %s", payload)
        else:
            payload = "status=OK smoke=clear"
            ok = await publish_one(host, port, "home/fire", 10, payload)
            if ok:
                log.info("🔥  Published: %s", payload)
        await asyncio.sleep(30.0)


async def door_sensor(host: str, port: int) -> None:
    """🚪 Publishes random door open/close events every 5-15 seconds."""
    log.info("🚪  Door Sensor started")
    doors = ["front", "back", "garage", "bedroom"]
    while True:
        door = random.choice(doors)
        state = random.choice(["OPEN", "CLOSED"])
        payload = f"door={state} location={door}"
        ok = await publish_one(host, port, "home/door", 128, payload)
        if ok:
            log.info("🚪  Published: %s", payload)
        await asyncio.sleep(random.uniform(5.0, 15.0))


async def smart_light(host: str, port: int) -> None:
    """💡 Publishes random light on/off events every 8-20 seconds."""
    log.info("💡  Smart Light started")
    rooms = ["living", "bedroom", "kitchen", "bathroom", "hallway"]
    while True:
        room = random.choice(rooms)
        state = random.choice(["ON", "OFF"])
        brightness = random.randint(10, 100)
        payload = f"light={state} room={room} brightness={brightness}%"
        ok = await publish_one(host, port, "home/light", 30, payload)
        if ok:
            log.info("💡  Published: %s", payload)
        await asyncio.sleep(random.uniform(8.0, 20.0))


# ── Main ─────────────────────────────────────────────────────────

async def run(host: str, port: int) -> None:
    log.info("=" * 60)
    log.info("  🏠 Smart Home IoT Device Simulator")
    log.info("  Broker: %s:%d", host, port)
    log.info("=" * 60)
    log.info("")
    log.info("  Devices:")
    log.info("    🌡️  Temperature Sensor  (every 2s,   priority 50)")
    log.info("    🔥  Fire Alarm          (every 30s,  priority 10/255)")
    log.info("    🚪  Door Sensor         (every 5-15s, priority 128)")
    log.info("    💡  Smart Light         (every 8-20s, priority 30)")
    log.info("")
    log.info("  Press Ctrl+C to stop all devices")
    log.info("=" * 60)

    await asyncio.gather(
        temperature_sensor(host, port),
        fire_alarm(host, port),
        door_sensor(host, port),
        smart_light(host, port),
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Smart Home IoT Device Simulator")
    p.add_argument("--host", default="127.0.0.1", help="Broker host (default: 127.0.0.1)")
    p.add_argument("--port", type=int, default=9999, help="Broker port (default: 9999)")
    args = p.parse_args()

    try:
        asyncio.run(run(args.host, args.port))
    except KeyboardInterrupt:
        log.info("All devices stopped")


if __name__ == "__main__":
    main()
