"""
Priority-Aware Routing (QoS Layer)
=============================================
Incoming published messages are enqueued into a **min-heap** managed by
``heapq``.  Because Python's heapq is a *min*-heap, we **negate** the
priority so that the *highest* priority value (255) is popped first.

A monotonic sequence counter breaks ties in FIFO order.
"""

from __future__ import annotations

import asyncio
import heapq
import logging
from typing import TYPE_CHECKING

from broker.protocol import Message, encode

if TYPE_CHECKING:
    pass  # only for forward refs

log = logging.getLogger(__name__)

__all__ = ["PriorityRouter"]


class PriorityRouter:
    """
    Async priority queue + background worker that fans messages out
    to all subscribers of the matching topic.
    """

    def __init__(self, registry) -> None:
        """
        Parameters
        ----------
        registry : ConnectionRegistry
            The live connection registry used to look up subscribers.
        """
        self._registry = registry
        self._heap: list[tuple[int, int, Message]] = []
        self._seq: int = 0                      # monotonic tie-breaker
        self._event = asyncio.Event()            # signals the worker
        self._running: bool = False
        self._worker_task: asyncio.Task | None = None

    # ── public API ────────────────────────────────────────────────

    def start(self) -> None:
        """Spawn the background worker task."""
        if self._running:
            return
        self._running = True
        self._worker_task = asyncio.create_task(self._worker())
        log.info("PriorityRouter worker started")

    async def stop(self) -> None:
        """Cancel the background worker and drain remaining items."""
        self._running = False
        self._event.set()                       # unblock the worker
        if self._worker_task is not None:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        log.info("PriorityRouter worker stopped")

    def enqueue(self, message: Message) -> None:
        """
        Push a message onto the priority heap.

        Priority is negated so that ``heapq`` (a min-heap) pops
        the *highest* priority first.
        """
        entry = (-message.priority, self._seq, message)
        self._seq += 1
        heapq.heappush(self._heap, entry)
        self._event.set()                       # wake up the worker

    @property
    def is_running(self) -> bool:
        """Return ``True`` when the background router worker is active."""
        return self._running

    @property
    def pending_count(self) -> int:
        """Return the number of messages waiting in the priority queue."""
        return len(self._heap)

    # ── internal worker ───────────────────────────────────────────

    async def _worker(self) -> None:
        """
        Background coroutine that waits for new messages, pops them
        in priority order, and fans out to matching subscribers.
        """
        log.info("Router worker loop running")
        while self._running:
            try:
                await self._event.wait()
                self._event.clear()

                while self._heap:
                    neg_pri, _seq, msg = heapq.heappop(self._heap)
                    await self._fanout(msg)
                    
                    # Tiny artificial delay to visualize QoS priority sorting 
                    # and prevent overwhelming the frontend visualization
                    await asyncio.sleep(0.05)
            except Exception as e:
                log.error(f"Router worker crashed: {e}", exc_info=True)
                await asyncio.sleep(1)  # prevent tight error loop

    async def _fanout(self, msg: Message) -> None:
        """
        Send *msg* to every subscriber registered for ``msg.topic_id``.
        Dead connections are silently unregistered.
        """
        subscribers = self._registry.get_subscribers(msg.topic_id)
        if not subscribers:
            log.debug("No subscribers for topic %d — message dropped", msg.topic_id)
            return

        frame = encode(msg.command, msg.topic_id, msg.priority, msg.payload)
        dead: list[str] = []

        for client_id, writer in subscribers:
            try:
                writer.write(frame)
                await asyncio.wait_for(writer.drain(), timeout=5.0)
            except (ConnectionResetError, ConnectionAbortedError,
                    BrokenPipeError, OSError, asyncio.TimeoutError) as exc:
                log.warning("Client %s unreachable (%s) — unregistering", client_id, exc)
                dead.append(client_id)

        for client_id in dead:
            await self._registry.unregister(client_id)
