"""
Unit Tests — Priority-Aware Routing (QoS Layer)
"""

import asyncio
import unittest

from broker.protocol import CMD_PUBLISH, Message, encode
from broker.router import PriorityRouter


class FakeRegistry:
    """Minimal stand-in for ConnectionRegistry used in tests."""

    def __init__(self):
        self._subs: dict[int, list[tuple[str, asyncio.StreamWriter]]] = {}
        self.unregistered: list[str] = []

    def subscribe(self, client_id: str, topic_id: int, writer: asyncio.StreamWriter):
        self._subs.setdefault(topic_id, []).append((client_id, writer))

    def get_subscribers(self, topic_id: int) -> list[tuple[str, asyncio.StreamWriter]]:
        return self._subs.get(topic_id, [])

    async def unregister(self, client_id: str):
        self.unregistered.append(client_id)


class TestPriorityOrdering(unittest.TestCase):
    """Verify that messages are delivered highest-priority-first."""

    def test_highest_priority_first(self):
        """Enqueue three messages with different priorities;
        they must be delivered 255 → 128 → 0."""

        delivered: list[int] = []

        class RecordingRegistry:
            def get_subscribers(self, topic_id):
                return [("c1", _make_writer())]

            async def unregister(self, cid):
                pass

        def _make_writer():
            """Create a fake writer that records written priorities."""
            class FakeTransport:
                def get_extra_info(self, *a): return None
                def is_closing(self): return False

            class FakeWriter:
                transport = FakeTransport()
                def write(self, data):
                    # Parse the priority from the encoded frame (byte 5)
                    from broker.protocol import decode_header
                    _, _, pri, _ = decode_header(data)
                    delivered.append(pri)
                async def drain(self):
                    pass
                def is_closing(self):
                    return False

            return FakeWriter()

        async def run():
            reg = RecordingRegistry()
            router = PriorityRouter(reg)

            # Enqueue LOW, MEDIUM, HIGH — should come out HIGH, MEDIUM, LOW
            router.enqueue(Message(CMD_PUBLISH, 1, 0, b"low"))
            router.enqueue(Message(CMD_PUBLISH, 1, 128, b"med"))
            router.enqueue(Message(CMD_PUBLISH, 1, 255, b"high"))

            router.start()
            await asyncio.sleep(0.3)   # let the worker process
            await router.stop()

        asyncio.run(run())
        self.assertEqual(delivered, [255, 128, 0])

    def test_fifo_within_same_priority(self):
        """Messages with equal priority must be delivered in FIFO order."""

        payloads: list[bytes] = []

        class RecordingRegistry:
            def get_subscribers(self, topic_id):
                return [("c1", _make_writer())]

            async def unregister(self, cid):
                pass

        def _make_writer():
            from broker.protocol import HEADER_SIZE

            class FakeWriter:
                def write(self, data):
                    payloads.append(data[HEADER_SIZE:])
                async def drain(self):
                    pass
                def is_closing(self):
                    return False

            return FakeWriter()

        async def run():
            reg = RecordingRegistry()
            router = PriorityRouter(reg)

            router.enqueue(Message(CMD_PUBLISH, 1, 100, b"first"))
            router.enqueue(Message(CMD_PUBLISH, 1, 100, b"second"))
            router.enqueue(Message(CMD_PUBLISH, 1, 100, b"third"))

            router.start()
            await asyncio.sleep(0.3)
            await router.stop()

        asyncio.run(run())
        self.assertEqual(payloads, [b"first", b"second", b"third"])

    def test_no_subscribers_no_crash(self):
        """Publishing to a topic with no subscribers must not raise."""

        class EmptyRegistry:
            def get_subscribers(self, topic_id):
                return []

            async def unregister(self, cid):
                pass

        async def run():
            router = PriorityRouter(EmptyRegistry())
            router.enqueue(Message(CMD_PUBLISH, 42, 100, b"lonely"))
            router.start()
            await asyncio.sleep(0.1)
            await router.stop()

        asyncio.run(run())  # should not raise


class TestRouterLifecycle(unittest.TestCase):
    """Verify start/stop semantics."""

    def test_double_start_is_safe(self):
        class Reg:
            def get_subscribers(self, t): return []
            async def unregister(self, c): pass

        async def run():
            r = PriorityRouter(Reg())
            r.start()
            r.start()  # second call should be a no-op
            await asyncio.sleep(0.05)
            await r.stop()

        asyncio.run(run())

    def test_stop_without_start(self):
        class Reg:
            def get_subscribers(self, t): return []
            async def unregister(self, c): pass

        async def run():
            r = PriorityRouter(Reg())
            await r.stop()  # should not raise

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
