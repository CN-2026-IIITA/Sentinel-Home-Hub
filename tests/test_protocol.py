"""
Unit Tests — Binary Protocol (Codec Layer)
"""

import asyncio
import struct
import unittest

from broker.protocol import (
    CMD_PUBLISH,
    CMD_SUBSCRIBE,
    CMD_TIME_TRAVEL,
    HEADER_FORMAT,
    HEADER_SIZE,
    Message,
    decode_header,
    encode,
    read_message,
    topic_hash,
)


class TestHeaderConstants(unittest.TestCase):
    """Verify the wire-format constants are correct."""

    def test_header_size_is_10(self):
        self.assertEqual(HEADER_SIZE, 10)

    def test_header_format_matches_size(self):
        self.assertEqual(struct.calcsize(HEADER_FORMAT), HEADER_SIZE)

    def test_command_values(self):
        self.assertEqual(CMD_SUBSCRIBE, 0x01)
        self.assertEqual(CMD_PUBLISH, 0x02)
        self.assertEqual(CMD_TIME_TRAVEL, 0x03)


class TestTopicHash(unittest.TestCase):
    """Verify topic_hash produces stable, valid uint32 values."""

    def test_deterministic(self):
        self.assertEqual(topic_hash("sensors"), topic_hash("sensors"))

    def test_different_topics_differ(self):
        self.assertNotEqual(topic_hash("sensors"), topic_hash("alerts"))

    def test_fits_in_uint32(self):
        h = topic_hash("any_topic_string")
        self.assertGreaterEqual(h, 0)
        self.assertLessEqual(h, 0xFFFFFFFF)


class TestEncode(unittest.TestCase):
    """Verify encode() produces correct wire-format frames."""

    def test_empty_payload(self):
        frame = encode(CMD_SUBSCRIBE, 42, 0)
        self.assertEqual(len(frame), HEADER_SIZE)
        cmd, tid, pri, plen = decode_header(frame)
        self.assertEqual(cmd, CMD_SUBSCRIBE)
        self.assertEqual(tid, 42)
        self.assertEqual(pri, 0)
        self.assertEqual(plen, 0)

    def test_with_payload(self):
        payload = b"hello world"
        frame = encode(CMD_PUBLISH, 100, 200, payload)
        self.assertEqual(len(frame), HEADER_SIZE + len(payload))

        cmd, tid, pri, plen = decode_header(frame)
        self.assertEqual(cmd, CMD_PUBLISH)
        self.assertEqual(tid, 100)
        self.assertEqual(pri, 200)
        self.assertEqual(plen, len(payload))
        self.assertEqual(frame[HEADER_SIZE:], payload)

    def test_max_priority(self):
        frame = encode(CMD_PUBLISH, 1, 255, b"x")
        _, _, pri, _ = decode_header(frame)
        self.assertEqual(pri, 255)


class TestDecodeHeader(unittest.TestCase):
    """Verify decode_header() handles edge cases."""

    def test_too_short_raises(self):
        with self.assertRaises(ValueError):
            decode_header(b"\x00" * 5)

    def test_roundtrip(self):
        frame = encode(CMD_TIME_TRAVEL, 999, 128, b"data")
        cmd, tid, pri, plen = decode_header(frame)
        self.assertEqual(cmd, CMD_TIME_TRAVEL)
        self.assertEqual(tid, 999)
        self.assertEqual(pri, 128)
        self.assertEqual(plen, 4)


class TestReadMessage(unittest.TestCase):
    """Verify async read_message correctly reads from a stream.

    NOTE: Python 3.13 requires ``asyncio.StreamReader`` to be created
    inside a running event loop, so we build readers inside coroutines.
    """

    @staticmethod
    def _make_reader(data: bytes) -> asyncio.StreamReader:
        """Must be called inside a running event loop."""
        reader = asyncio.StreamReader()
        reader.feed_data(data)
        reader.feed_eof()
        return reader

    def test_read_subscribe(self):
        async def go():
            frame = encode(CMD_SUBSCRIBE, 42, 0)
            reader = self._make_reader(frame)
            return await read_message(reader)

        msg = asyncio.run(go())
        self.assertIsNotNone(msg)
        self.assertEqual(msg.command, CMD_SUBSCRIBE)
        self.assertEqual(msg.topic_id, 42)
        self.assertEqual(msg.payload, b"")

    def test_read_publish_with_payload(self):
        payload = b"temperature=42.5"

        async def go():
            frame = encode(CMD_PUBLISH, 100, 200, payload)
            reader = self._make_reader(frame)
            return await read_message(reader)

        msg = asyncio.run(go())
        self.assertEqual(msg.command, CMD_PUBLISH)
        self.assertEqual(msg.topic_id, 100)
        self.assertEqual(msg.priority, 200)
        self.assertEqual(msg.payload, payload)

    def test_read_multiple_messages(self):
        async def go():
            f1 = encode(CMD_PUBLISH, 1, 10, b"aaa")
            f2 = encode(CMD_PUBLISH, 2, 20, b"bbb")
            reader = self._make_reader(f1 + f2)
            m1 = await read_message(reader)
            m2 = await read_message(reader)
            return m1, m2

        m1, m2 = asyncio.run(go())
        self.assertEqual(m1.topic_id, 1)
        self.assertEqual(m2.topic_id, 2)
        self.assertEqual(m1.payload, b"aaa")
        self.assertEqual(m2.payload, b"bbb")

    def test_incomplete_header_raises(self):
        async def go():
            reader = self._make_reader(b"\x01\x02")  # too short
            return await read_message(reader)

        with self.assertRaises(asyncio.IncompleteReadError):
            asyncio.run(go())


if __name__ == "__main__":
    unittest.main()
