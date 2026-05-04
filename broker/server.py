"""
Asynchronous Core (Network Layer)
=================================
High-concurrency TCP server built on asyncio.

Responsibilities
----------------
- Accept TCP clients and keep their connections alive.
- Maintain a subscription registry for topic-based routing.
- Decode binary frames and route them to storage/router.
- Handle disconnects without crashing the event loop.
"""


from __future__ import annotations

import asyncio
import json
import logging
import struct
import uuid
from typing import Optional

from broker.protocol import (
    CMD_PUBLISH,
    CMD_SUBSCRIBE,
    CMD_TIME_TRAVEL,
    Message,
    encode,
    read_message,
    topic_hash,
)
from broker.router import PriorityRouter
from broker.storage import EventLog

log = logging.getLogger(__name__)

MAX_PAYLOAD_BYTES = 5 * 1024 * 1024
TIME_TRAVEL_OFFSET_BYTES = 8


class ConnectionRegistry:
    def __init__(self) -> None:
        self._clients: dict[str, tuple[asyncio.StreamReader,
                                       asyncio.StreamWriter,
                                       set[int]]] = {}

    async def register(
        self,
        client_id: str,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self._clients[client_id] = (reader, writer, set())
        addr = writer.get_extra_info("peername")
        log.info("Client %s connected from %s  [total: %d]",
                 client_id, addr, len(self._clients))

    async def unregister(self, client_id: str) -> None:
        entry = self._clients.pop(client_id, None)
        if entry is not None:
            _reader, writer, _topics = entry
            if not writer.is_closing():
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
            log.info("Client %s disconnected  [total: %d]",
                     client_id, len(self._clients))

    def subscribe(self, client_id: str, topic_id: int) -> None:
        if client_id in self._clients:
            self._clients[client_id][2].add(topic_id)
            log.info("Client %s subscribed to topic %d", client_id, topic_id)

    def get_subscribers(self, topic_id: int) -> list[tuple[str, asyncio.StreamWriter]]:
        result: list[tuple[str, asyncio.StreamWriter]] = []
        for cid, (_, writer, topics) in self._clients.items():
            if topic_id in topics:
                result.append((cid, writer))
        return result

    @property
    def count(self) -> int:
        return len(self._clients)


class BrokerServer:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 9999,
        log_path: str = "broker_log.bin",
    ) -> None:
        self.host = host
        self.port = port
        self.registry = ConnectionRegistry()
        self.event_log = EventLog(path=log_path)
        self.router = PriorityRouter(registry=self.registry)
        self._server: Optional[asyncio.Server] = None
        self._shutdown_event = asyncio.Event()
        self.messages_processed = 0
        self.latest_offset = 0
        self._metrics_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self.router.start()
        self._metrics_task = asyncio.create_task(self._publish_metrics())

        self._server = await asyncio.start_server(
            self._handle_client, self.host, self.port
        )

        addrs = ", ".join(str(s.getsockname()) for s in self._server.sockets)
        log.info("Broker listening on %s", addrs)

        async with self._server:
            await self._shutdown_event.wait()

    async def stop(self) -> None:
        log.info("Broker shutting down…")
        self._shutdown_event.set()

        if self._metrics_task:
            self._metrics_task.cancel()

        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

        await self.router.stop()

    async def _publish_metrics(self) -> None:
        topic_id = topic_hash("broker/metrics")
        try:
            while not self._shutdown_event.is_set():
                await asyncio.sleep(1.0)

                throughput = self.messages_processed
                self.messages_processed = 0

                metrics_data = {
                    "throughput": throughput,
                    "current_log_offset": self.latest_offset,
                    "active_clients": self.registry.count,
                }

                payload = json.dumps(metrics_data).encode("utf-8")
                msg = Message(
                    command=CMD_PUBLISH,
                    topic_id=topic_id,
                    priority=255,
                    payload=payload,
                )
                self.router.enqueue(msg)
        except asyncio.CancelledError:
            pass

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        client_id = uuid.uuid4().hex[:12]
        await self.registry.register(client_id, reader, writer)

        try:
            while True:
                try:
                    msg = await read_message(reader)
                except asyncio.IncompleteReadError:
                    break

                if msg is None:
                    break

                if len(msg.payload) > MAX_PAYLOAD_BYTES:
                    log.warning("Payload too large from %s (%d bytes)",
                                client_id, len(msg.payload))
                    break

                await self._dispatch(client_id, msg, writer)

        except (ConnectionResetError, ConnectionAbortedError,
                BrokenPipeError, OSError) as exc:
            log.debug("Client %s connection error: %s", client_id, exc)
        finally:
            await self.registry.unregister(client_id)

    async def _dispatch(
        self,
        client_id: str,
        msg: Message,
        writer: asyncio.StreamWriter,
    ) -> None:
        if msg.command == CMD_SUBSCRIBE:
            await self._handle_subscribe(client_id, msg) 
        elif msg.command == CMD_PUBLISH:
            await self._handle_publish(msg)
        elif msg.command == CMD_TIME_TRAVEL:
            await self._handle_time_travel(client_id, msg, writer)
        else:
            log.warning("Unknown command 0x%02x from %s", msg.command, client_id)

    async def _handle_subscribe(self, client_id: str, msg: Message) -> None:
        self.registry.subscribe(client_id, msg.topic_id)

    async def _handle_publish(self, msg: Message) -> None:
        frame = encode(msg.command, msg.topic_id, msg.priority, msg.payload)
        offset = await self.event_log.append(frame)
        self.latest_offset = offset + len(frame)
        self.messages_processed += 1

        log.info(
            "PUBLISH  topic=%d  pri=%d  len=%d  offset=%d",
            msg.topic_id, msg.priority, len(msg.payload), offset,
        )

        self.router.enqueue(msg)

    async def _handle_time_travel(
        self,
        client_id: str,
        msg: Message,
        writer: asyncio.StreamWriter,
    ) -> None:
        if len(msg.payload) < TIME_TRAVEL_OFFSET_BYTES:
            log.warning("Time-travel payload too short from %s", client_id)
            return

        offset = struct.unpack("!Q", msg.payload[:8])[0]
        log.info("TIME-TRAVEL  client=%s  topic=%d  from_offset=%d",
                 client_id, msg.topic_id, offset)

        count = 0
        try:
            async for frame in self.event_log.replay_from(offset):
                writer.write(frame)
                await writer.drain()
                count += 1
        except (ConnectionResetError, BrokenPipeError, OSError):
            log.warning("Client %s disconnected during time-travel replay", client_id)
            return

        log.info("Replayed %d historical messages to %s", count, client_id)