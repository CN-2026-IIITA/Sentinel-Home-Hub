"""
Integration Test — End-to-End Broker Test
==========================================
Starts the broker in-process, then uses async TCP clients to:

1. Subscribe a client to a topic.
2. Publish messages with varying priorities.
3. Verify messages arrive in priority order.
4. Verify time-travel replay delivers historical messages.
5. Verify graceful disconnect does not crash the broker.
"""

import asyncio
import os
import struct
import tempfile
import unittest

from broker.protocol import (
    CMD_PUBLISH,
    CMD_SUBSCRIBE,
    CMD_TIME_TRAVEL,
    HEADER_SIZE,
    decode_header,
    encode,
    topic_hash,
)
from broker.server import BrokerServer


async def _read_messages(reader, count, timeout=2.0):
    """Helper: read *count* messages from *reader* with a global timeout."""
    received = []
    try:
        async with asyncio.timeout(timeout):
            for _ in range(count):
                header = await reader.readexactly(HEADER_SIZE)
                _, _, pri, plen = decode_header(header)
                payload = await reader.readexactly(plen) if plen else b""
                received.append((pri, payload))
    except (asyncio.TimeoutError, asyncio.IncompleteReadError):
        pass
    return received


class TestPubSubPriorityOrder(unittest.TestCase):
    """
    Publish 3 messages at priorities 50, 200, 100.
    Subscriber should receive them in order: 200, 100, 50.

    Strategy: delay the router worker start until all three
    messages have been enqueued so they are sorted together.
    """

    def test_priority_ordering(self):

        async def run():
            tmpdir = tempfile.mkdtemp()
            log_path = os.path.join(tmpdir, "pri_log.bin")

            server = BrokerServer(host="127.0.0.1", port=0, log_path=log_path)

            # Do NOT start the router worker yet — we want all
            # messages in the heap before it pops any.
            tcp_server = await asyncio.start_server(
                server._handle_client, "127.0.0.1", 0,
            )
            host, port = tcp_server.sockets[0].getsockname()

            try:
                # ── Subscriber connects & subscribes ──
                tid = topic_hash("priority_test")
                sub_r, sub_w = await asyncio.open_connection(host, port)
                sub_w.write(encode(CMD_SUBSCRIBE, tid, 0))
                await sub_w.drain()
                await asyncio.sleep(0.05)

                # ── Publisher connects & sends 3 messages ──
                pub_r, pub_w = await asyncio.open_connection(host, port)
                for pri, text in [(50, b"low"), (200, b"high"), (100, b"med")]:
                    pub_w.write(encode(CMD_PUBLISH, tid, pri, text))
                await pub_w.drain()

                # Close publisher so _handle_client finishes reading
                pub_w.close()
                await pub_w.wait_closed()

                # Wait for all 3 publishes to be handled & enqueued
                await asyncio.sleep(0.3)

                # NOW start the router worker — all 3 sit in the heap
                server.router.start()
                await asyncio.sleep(0.3)

                # ── Read delivered messages ──
                received = await _read_messages(sub_r, 3, timeout=2.0)

                priorities = [r[0] for r in received]
                self.assertEqual(priorities, [200, 100, 50],
                                 f"Expected [200, 100, 50], got {received}")

                sub_w.close()
                await sub_w.wait_closed()
            finally:
                await server.router.stop()
                tcp_server.close()
                await tcp_server.wait_closed()
                if os.path.exists(log_path):
                    os.remove(log_path)
                os.rmdir(tmpdir)

        asyncio.run(run())


class TestTimeTravelReplay(unittest.TestCase):
    """
    Publish messages, then connect a new client with
    CMD_TIME_TRAVEL offset=0 and verify all history is replayed.
    """

    def test_replay_from_offset_zero(self):

        async def run():
            tmpdir = tempfile.mkdtemp()
            log_path = os.path.join(tmpdir, "tt_log.bin")

            server = BrokerServer(host="127.0.0.1", port=0, log_path=log_path)
            server.router.start()
            tcp_server = await asyncio.start_server(
                server._handle_client, "127.0.0.1", 0,
            )
            host, port = tcp_server.sockets[0].getsockname()
            tid = topic_hash("history")

            try:
                # ── Publish 3 messages (no subscribers) ──
                pub_r, pub_w = await asyncio.open_connection(host, port)
                for i in range(3):
                    pub_w.write(encode(CMD_PUBLISH, tid, 100, f"msg-{i}".encode()))
                await pub_w.drain()
                pub_w.close()
                await pub_w.wait_closed()
                await asyncio.sleep(0.2)

                # ── Time-travel client connects at offset 0 ──
                tt_r, tt_w = await asyncio.open_connection(host, port)
                offset_payload = struct.pack("!Q", 0)
                tt_w.write(encode(CMD_TIME_TRAVEL, tid, 0, offset_payload))
                await tt_w.drain()
                await asyncio.sleep(0.3)

                # ── Read replayed messages ──
                replayed = await _read_messages(tt_r, 3, timeout=2.0)
                payloads = [r[1] for r in replayed]

                self.assertEqual(len(payloads), 3, f"Expected 3, got {payloads}")
                self.assertEqual(payloads[0], b"msg-0")
                self.assertEqual(payloads[1], b"msg-1")
                self.assertEqual(payloads[2], b"msg-2")

                tt_w.close()
                await tt_w.wait_closed()
            finally:
                await server.router.stop()
                tcp_server.close()
                await tcp_server.wait_closed()
                if os.path.exists(log_path):
                    os.remove(log_path)
                os.rmdir(tmpdir)

        asyncio.run(run())


class TestGracefulDisconnect(unittest.TestCase):
    """
    A subscriber disconnecting mid-session must not crash
    the broker or affect other subscribers.
    """

    def test_disconnect_does_not_crash(self):

        async def run():
            tmpdir = tempfile.mkdtemp()
            log_path = os.path.join(tmpdir, "dc_log.bin")

            server = BrokerServer(host="127.0.0.1", port=0, log_path=log_path)
            server.router.start()
            tcp_server = await asyncio.start_server(
                server._handle_client, "127.0.0.1", 0,
            )
            host, port = tcp_server.sockets[0].getsockname()
            tid = topic_hash("dc_test")

            try:
                # Sub 1 — will disconnect early
                s1_r, s1_w = await asyncio.open_connection(host, port)
                s1_w.write(encode(CMD_SUBSCRIBE, tid, 0))
                await s1_w.drain()

                # Sub 2 — stays alive
                s2_r, s2_w = await asyncio.open_connection(host, port)
                s2_w.write(encode(CMD_SUBSCRIBE, tid, 0))
                await s2_w.drain()
                await asyncio.sleep(0.05)

                # Kill sub 1
                s1_w.close()
                await s1_w.wait_closed()
                await asyncio.sleep(0.05)

                # Publish — should reach sub 2 without crashing
                pub_r, pub_w = await asyncio.open_connection(host, port)
                pub_w.write(encode(CMD_PUBLISH, tid, 128, b"still_alive"))
                await pub_w.drain()
                pub_w.close()
                await pub_w.wait_closed()
                await asyncio.sleep(0.3)

                # Sub 2 should have received the message
                received = await _read_messages(s2_r, 1, timeout=2.0)
                self.assertEqual(len(received), 1, f"Expected 1 message, got {received}")
                self.assertEqual(received[0][1], b"still_alive")

                s2_w.close()
                await s2_w.wait_closed()
            finally:
                await server.router.stop()
                tcp_server.close()
                await tcp_server.wait_closed()
                if os.path.exists(log_path):
                    os.remove(log_path)
                os.rmdir(tmpdir)

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
