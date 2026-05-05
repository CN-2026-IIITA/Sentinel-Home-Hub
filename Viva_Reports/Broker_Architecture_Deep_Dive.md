# Code Deep-Dive: Broker Architecture (`protocol`, `storage`, `router`)

Even though your primary responsibility was `server.py`, a great engineer understands the entire system. Your server acts as the "Traffic Cop," but it relies on three specialized workers to do the heavy lifting: the Codec (`protocol.py`), the Hard Drive Manager (`storage.py`), and the Delivery Team (`router.py`).

Here is exactly how they work under the hood.

---

## 1. The Codec Layer: `protocol.py`
This file is responsible for translating human-readable data into incredibly tiny, efficient binary bytes for network transmission. It ensures the broker uses 70% less bandwidth than standard web apps.

* **`topic_hash()`:** Strings are heavy and slow to route. This function takes a string like `"home/temperature"` and runs it through an MD5 hashing algorithm to crush it into a tiny 4-byte integer. The broker routes these integers instead of strings to save massive CPU cycles.
* **`encode()`:** This is the packing machine. It takes your variables (Command, Topic ID, Priority, and Payload Length) and uses Python's `struct.pack("!BIBI")` to physically squash them into exactly **10 bytes**. It then glues the payload bytes to the end to create a final "frame."
* **`read_message()`:** This function is the ultimate defense against TCP bugs. Because TCP is a continuous stream of water (not distinct packets), messages can blur together. This function tells the OS: *"Read exactly 10 bytes. Stop. Decode them to find the payload length. Now read exactly that many payload bytes."* This prevents "packet sticking" under high stress.

---

## 2. The Event-Sourced Log: `storage.py`
This file is the memory of the broker. It handles the "Write-Ahead Log" pattern, ensuring no data is ever lost.

* **Thread-Pool Offloading:** Hard drives are much slower than RAM. If the main server paused to wait for the hard drive to spin, the whole network would lag. To fix this, the `EventLog` class wraps all file I/O in `asyncio.to_thread()`. This offloads the slow disk writing to background CPU threads, keeping your main `server.py` loop lightning fast.
* **`append()`:** Whenever a message arrives, this function opens `broker_log.bin` and blindly appends the raw binary bytes to the very end of the file. It returns the exact `offset` (e.g., "Byte #4,092"), which your server remembers as a bookmark.
* **`replay_from()`:** This is the Time-Travel engine. If a client asks for history starting at Byte #4,092, this function physically seeks to that spot on the hard drive. It reads the raw bytes in batches of 50 and yields them directly back to your server to be streamed to the client.

---

## 3. The QoS Delivery Team: `router.py`
This file is responsible for Quality of Service (QoS). It guarantees that emergency Fire Alarms get delivered before mundane battery updates, no matter how many messages are pending.

* **The Min-Heap (`heapq`):** Standard queues are First-In-First-Out (FIFO). This router uses a **Min-Heap**. Because Python's heap always pops the smallest number first, the code cleverly *negates* the priority. A Fire Alarm (Priority 255) becomes `-255`, ensuring it is mathematically the smallest number and always bubbles instantly to the top of the tree.
* **`enqueue()`:** When your server gets a message, it hands it here. The router drops the message into the Min-Heap and triggers an `asyncio.Event()` flag to wake up the delivery worker.
* **`_worker()`:** This is a permanent background task. When the flag wakes it up, it continuously pops the most critical message off the top of the heap and hands it to `_fanout()`.
* **`_fanout()`:** This function asks your `ConnectionRegistry` for a list of every active client listening to the message's topic. It loops through all their sockets and blasts the raw binary bytes down the TCP pipes. If a client's internet has died and the socket times out, this function catches the error and silently deletes them from the registry.
