"""
Event-Sourced Logging & "Time-Travel" (Storage Layer)
===============================================================
Every published message is appended to a binary log file
(``broker_log.bin``) before it is routed.  The log stores complete
wire-format frames so replaying is just reading + forwarding.

Time-Travel Subscribe
---------------------
A client can send ``CMD_TIME_TRAVEL`` with a byte offset.  The broker
seeks to that offset, streams every historical message to the client,
then seamlessly transitions the client to real-time delivery.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import AsyncIterator

from broker.protocol import HEADER_SIZE, decode_header

log = logging.getLogger(__name__)


class EventLog:
    """
    Append-only binary log backed by a single file.

    All I/O is delegated to a thread-pool via ``asyncio.to_thread()``
    so the event loop is never blocked.
    """

    def __init__(self, path: str = "broker_log.bin") -> None:
        self._path = path
        # Ensure the file exists
        if not os.path.exists(path):
            with open(path, "wb"):
                pass
        log.info("EventLog initialised at %s", os.path.abspath(path))

    # ── write path ────────────────────────────────────────────────

    async def append(self, frame: bytes) -> int:
        """
        Append a complete wire-format frame to the log.

        Parameters
        ----------
        frame : bytes
            The full encoded message (header + payload).

        Returns
        -------
        int
            The byte offset at which this frame was written
            (i.e. the "bookmark" for time-travel).
        """
        offset = await asyncio.to_thread(self._sync_append, frame)
        log.debug("Appended %d bytes at offset %d", len(frame), offset)
        return offset

    def _sync_append(self, frame: bytes) -> int:
        """Blocking append — runs in the default thread-pool."""
        with open(self._path, "ab") as f:
            offset = f.tell()
            f.write(frame)
            f.flush()
            return offset

    # ── read / replay path ────────────────────────────────────────

    async def replay_from(self, offset: int) -> AsyncIterator[bytes]:
        """
        Yield every stored frame starting from *offset*.

        Each frame is exactly ``HEADER_SIZE + payload_length`` bytes.
        """
        async for frame in self._async_reader(offset):
            yield frame

    async def _async_reader(self, offset: int) -> AsyncIterator[bytes]:
        """
        Read frames in batches, offloading blocking I/O
        to the thread pool for efficiency.
        """
        while True:
            results = await asyncio.to_thread(self._sync_read_batch, offset, 50)
            if not results:
                break
            for frame, next_offset in results:
                yield frame
                offset = next_offset

    def _sync_read_batch(self, offset: int, max_frames: int = 50) -> list[tuple[bytes, int]]:
        """
        Read up to *max_frames* frames starting at *offset*.

        Returns
        -------
        List of (frame_bytes, next_offset) tuples, empty list if EOF.
        """
        results = []
        with open(self._path, "rb") as f:
            f.seek(offset)
            for _ in range(max_frames):
                header_data = f.read(HEADER_SIZE)
                if len(header_data) < HEADER_SIZE:
                    break  # EOF

                _cmd, _topic, _pri, payload_len = decode_header(header_data)
                payload_data = f.read(payload_len)
                if len(payload_data) < payload_len:
                    break  # truncated frame

                frame = header_data + payload_data
                offset += len(frame)
                results.append((frame, offset))
        return results

    # ── utility ───────────────────────────────────────────────────

    async def size(self) -> int:
        """Return the current size of the log file in bytes."""
        return await asyncio.to_thread(os.path.getsize, self._path)
