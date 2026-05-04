"""
Unit Tests — Event-Sourced Logging & Time-Travel (Storage Layer)
"""

import asyncio
import os
import tempfile
import unittest

from broker.protocol import CMD_PUBLISH, HEADER_SIZE, decode_header, encode
from broker.storage import EventLog


class TestEventLogAppend(unittest.TestCase):
    """Verify append writes frames and returns correct offsets."""

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self._path = os.path.join(self._tmpdir, "test_log.bin")

    def tearDown(self):
        if os.path.exists(self._path):
            os.remove(self._path)
        os.rmdir(self._tmpdir)

    def test_first_write_at_offset_zero(self):
        async def run():
            log = EventLog(self._path)
            frame = encode(CMD_PUBLISH, 1, 100, b"hello")
            offset = await log.append(frame)
            self.assertEqual(offset, 0)

        asyncio.run(run())

    def test_sequential_offsets(self):
        async def run():
            log = EventLog(self._path)
            f1 = encode(CMD_PUBLISH, 1, 100, b"aaa")
            f2 = encode(CMD_PUBLISH, 2, 200, b"bbbbbb")

            o1 = await log.append(f1)
            o2 = await log.append(f2)

            self.assertEqual(o1, 0)
            self.assertEqual(o2, len(f1))

        asyncio.run(run())

    def test_file_grows(self):
        async def run():
            log = EventLog(self._path)
            f1 = encode(CMD_PUBLISH, 1, 0, b"x" * 100)
            await log.append(f1)
            size = await log.size()
            self.assertEqual(size, HEADER_SIZE + 100)

        asyncio.run(run())


class TestEventLogReplay(unittest.TestCase):
    """Verify replay_from yields the correct frames."""

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self._path = os.path.join(self._tmpdir, "test_log.bin")

    def tearDown(self):
        if os.path.exists(self._path):
            os.remove(self._path)
        os.rmdir(self._tmpdir)

    def test_replay_all_from_zero(self):
        async def run():
            log = EventLog(self._path)
            frames_in = [
                encode(CMD_PUBLISH, 1, 10, b"one"),
                encode(CMD_PUBLISH, 2, 20, b"two"),
                encode(CMD_PUBLISH, 3, 30, b"three"),
            ]
            for f in frames_in:
                await log.append(f)

            frames_out = []
            async for frame in log.replay_from(0):
                frames_out.append(frame)

            self.assertEqual(len(frames_out), 3)
            self.assertEqual(frames_out, frames_in)

        asyncio.run(run())

    def test_replay_from_middle(self):
        async def run():
            log = EventLog(self._path)
            f1 = encode(CMD_PUBLISH, 1, 10, b"skip")
            f2 = encode(CMD_PUBLISH, 2, 20, b"want_this")

            await log.append(f1)
            o2 = await log.append(f2)

            frames_out = []
            async for frame in log.replay_from(o2):
                frames_out.append(frame)

            self.assertEqual(len(frames_out), 1)
            self.assertEqual(frames_out[0], f2)

        asyncio.run(run())

    def test_replay_from_end_yields_nothing(self):
        async def run():
            log = EventLog(self._path)
            f1 = encode(CMD_PUBLISH, 1, 10, b"only")
            await log.append(f1)

            total_size = await log.size()
            frames_out = []
            async for frame in log.replay_from(total_size):
                frames_out.append(frame)

            self.assertEqual(len(frames_out), 0)

        asyncio.run(run())

    def test_replay_preserves_content(self):
        """Ensure replayed frame payloads match originals exactly."""

        async def run():
            log = EventLog(self._path)
            payload = b"binary\x00data\xff\xfe"
            f = encode(CMD_PUBLISH, 99, 255, payload)
            await log.append(f)

            async for frame in log.replay_from(0):
                _, _, _, plen = decode_header(frame)
                self.assertEqual(frame[HEADER_SIZE:], payload)

        asyncio.run(run())


class TestEventLogEmpty(unittest.TestCase):
    """Edge-case: empty log file."""

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self._path = os.path.join(self._tmpdir, "empty_log.bin")

    def tearDown(self):
        if os.path.exists(self._path):
            os.remove(self._path)
        os.rmdir(self._tmpdir)

    def test_replay_empty_log(self):
        async def run():
            log = EventLog(self._path)
            frames = []
            async for frame in log.replay_from(0):
                frames.append(frame)
            self.assertEqual(frames, [])

        asyncio.run(run())

    def test_size_of_empty_log(self):
        async def run():
            log = EventLog(self._path)
            self.assertEqual(await log.size(), 0)

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
