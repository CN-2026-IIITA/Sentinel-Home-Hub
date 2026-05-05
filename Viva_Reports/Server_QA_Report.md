# Anticipated Professor Q&A: Focus on `broker/server.py`

Since your specific duty was the core network server (`broker/server.py`), the evaluators will aggressively drill into your code to ensure you didn't just copy/paste an existing server. 

Here are the 10 toughest, most targeted questions a professor will ask **specifically about your file**, along with the perfect technical answers to prove you wrote and understand every line.

---

### Q1: "In `_handle_publish`, why do you append the message to the `event_log` *before* you send it to the router?"
**The Professor's Goal:** Checking if you understand data integrity and the Write-Ahead Log (WAL) pattern.
**Your Answer:** 
> "This is a critical architectural decision to prevent data loss. If we routed the message first, and the broker crashed mid-delivery, that message would be permanently lost. By forcing the server to `await self.event_log.append(frame)` *first*, we guarantee the raw bytes are physically flushed to the hard drive (`broker_log.bin`) before we do anything else. If the server crashes 1 millisecond later, the data is still safe on disk and can be recovered."

### Q2: "How does your `ConnectionRegistry` prevent memory leaks when a client's internet connection drops unexpectedly?"
**The Professor's Goal:** Checking your understanding of socket lifecycle management and resource cleanup.
**Your Answer:** 
> "When a client connects, we spawn `_handle_client()`. Inside that function, the infinite reading loop is wrapped in a `try...except` block that catches network exceptions like `ConnectionResetError` or `BrokenPipeError`. Crucially, we use a `finally:` block to call `await self.registry.unregister(client_id)`. This guarantees that whether the client disconnects politely, or their internet cable is violently pulled out, our server will *always* delete their socket from memory and prevent ghost connections from leaking RAM."

### Q3: "Why did you use Python's `asyncio.start_server` instead of the lower-level `socket` library?"
**The Professor's Goal:** Seeing if you understand the abstractions you used.
**Your Answer:** 
> "Using raw `socket.listen()` would have forced me to manually write complex `select()` or `epoll()` polling loops to handle multiple connections concurrently without blocking. By using `asyncio.start_server`, Python's event loop automatically handles the low-level OS multiplexing for me. It seamlessly provides a non-blocking `StreamReader` and `StreamWriter` for every client, allowing our single-threaded server to handle thousands of connections concurrently using minimal CPU."

### Q4: "In your Time Travel function (`_handle_time_travel`), you read history from the disk and send it to the client. Doesn't reading a massive file block the entire server and freeze other clients?"
**The Professor's Goal:** Checking your understanding of asynchronous I/O and event loop blocking.
**Your Answer:** 
> "No, it doesn't block the server because we use an Asynchronous Generator (`async for frame in self.event_log.replay_from`). When reading the binary log from the disk, the server yields control back to the `asyncio` event loop between every single frame read. This means while Client A is downloading 10,000 historical messages, the server is constantly pausing to accept new connections and route live messages for Client B simultaneously."

### Q5: "What stops a malicious sensor from sending a 5 Gigabyte payload and causing an Out-Of-Memory (OOM) crash on your broker?"
**The Professor's Goal:** Checking application-layer security.
**Your Answer:** 
> "We implemented strict byte limits exactly for this scenario. In the client reading loop, the moment we decode the 10-byte header, we check the parsed payload length against our `MAX_PAYLOAD_BYTES` constant (which is set to 5 MB). If the header claims the payload is larger than that, we instantly break the loop and close the socket *before* calling `reader.readexactly()`. This completely neutralizes the attack before the OS even allocates memory for the massive payload."

### Q6: "Your server tracks metrics like 'messages_processed'. How do you broadcast these metrics without interrupting the main server loop?"
**The Professor's Goal:** Checking how you handle periodic background tasks.
**Your Answer:** 
> "In the `start()` method, I spawn a background task using `asyncio.create_task(self._publish_metrics())`. This function runs in an infinite loop that simply `await asyncio.sleep(1.0)`, wakes up, calculates the throughput, injects a system-level message (`broker/metrics`) into the Priority Router, and goes back to sleep. Because it uses `await`, it doesn't hog the CPU, and the main server loop doesn't even know it exists."

### Q7: "I see you catch `asyncio.IncompleteReadError`. Why does that happen, and why is it important to catch it?"
**The Professor's Goal:** Checking your understanding of TCP fragmentation and dirty disconnects.
**Your Answer:** 
> "Because TCP is a continuous stream, a client's internet might die exactly halfway through transmitting the 10-byte header. If that happens, `reader.readexactly(10)` throws an `IncompleteReadError` because the OS buffer closed before 10 bytes arrived. If we didn't explicitly catch this error, that single broken sensor would crash the entire Python process. Catching it allows us to gracefully drop that specific client while the rest of the broker stays alive."

### Q8: "You have hundreds of clients adding and removing themselves from the `ConnectionRegistry` dictionary simultaneously. Why didn't you use a `Mutex` or `Lock`? Isn't that a race condition?"
**The Professor's Goal:** Checking if you understand the fundamental difference between Asyncio and Multi-threading.
**Your Answer:** 
> "If we were using standard Multi-threading, yes, we would need a Thread Lock to prevent memory corruption. However, `asyncio` is completely single-threaded and uses 'cooperative multitasking'. Because modifying a Python dictionary is a synchronous operation, the event loop can never context-switch exactly in the middle of a dictionary update. Therefore, data race conditions are physically impossible in our `ConnectionRegistry`, removing the need for slow Mutex locks."

### Q9: "When I press Ctrl+C, the broker doesn't just crash instantly. How did you implement a graceful shutdown in `stop()`?"
**The Professor's Goal:** Checking your understanding of system lifecycles.
**Your Answer:** 
> "A hard crash would corrupt our append-only binary log. Instead, we catch the `KeyboardInterrupt` and call `stop()`. This triggers an `asyncio.Event` flag (`_shutdown_event.set()`), which elegantly kills our background metrics task. We then close the main TCP listening socket so no new clients can connect, and finally, we `await self.router.stop()` to ensure the Min-Heap completely drains and routes its remaining priority messages before we safely kill the event loop."

### Q10: "How does the server actually know the IP address of the IoT devices connecting to it for logging purposes?"
**The Professor's Goal:** Checking if you know how to interact with the underlying OS socket APIs.
**Your Answer:** 
> "When `asyncio` accepts a connection, it gives us a high-level `StreamWriter` wrapper. To get the raw network data, we call `writer.get_extra_info('peername')`. This drops down to the underlying C-socket API and asks the Operating System for the remote IP address and port that established the TCP handshake. We use this strictly for the `logging.info` output when a client joins."
