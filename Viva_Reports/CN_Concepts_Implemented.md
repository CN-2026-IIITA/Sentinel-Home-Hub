# Computer Networks (CN) Concepts Implementation Report

The Sentinel Home Hub is an excellent project for a Computer Networks curriculum because it abandons high-level abstractions (like HTTP/REST) in favor of low-level, high-performance socket programming. 

Here is a breakdown of every major CN concept used in your project and exactly where it is implemented in the codebase:

### 1. Transport Layer: Persistent TCP Sockets
*   **The Concept:** Reliable, ordered, and error-checked delivery of a stream of bytes between hosts.
*   **Where it's Implemented:** `broker/server.py` binds a raw TCP socket to port 9999 using `asyncio.start_server`. Clients (`client.py`, `simulate_load.py`) establish long-lived, persistent connections using `asyncio.open_connection`. This avoids the expensive "three-way handshake" overhead of opening a new socket for every single message.

### 2. Application Layer: Custom Binary Protocol
*   **The Concept:** Designing an optimized wire-protocol from scratch rather than relying on bloated text-based protocols like HTTP or JSON.
*   **Where it's Implemented:** `broker/protocol.py`. You built a custom 10-byte binary header (`!BIBI` struct format). Instead of sending long strings like `"home/temperature"` over the network, you use the MD5 hash algorithm to crush the topic down to a 4-byte integer. This ensures maximum bandwidth efficiency.

### 3. Publish/Subscribe (Pub/Sub) Architecture
*   **The Concept:** A messaging pattern that decouples the senders (publishers) from the receivers (subscribers), allowing for highly scalable network topologies.
*   **Where it's Implemented:** The `ConnectionRegistry` class in `broker/server.py`. It maintains a dynamic routing table in memory, mapping specific active TCP sockets to the `topic_id`s they are interested in.

### 4. Quality of Service (QoS) & Traffic Prioritization
*   **The Concept:** Network traffic management that prioritizes critical packets over background noise when a bottleneck or congestion occurs.
*   **Where it's Implemented:** `broker/router.py`. You implemented an asynchronous Min-Heap (`asyncio.PriorityQueue`). When the network is flooded (as simulated by `simulate_load.py`), the Min-Heap mathematically guarantees that Priority 255 packets (Fire Alarms) jump to the front of the queue and are broadcasted before Priority 0 packets (Battery updates).

### 5. Multiplexing & Demultiplexing
*   **The Concept:** Combining multiple logical data streams over a single physical medium (a single TCP connection) to prevent "socket exhaustion" on the OS.
*   **Where it's Implemented:** Both `client.py` and `dashboard_server.py`. They send a single comma-separated topic list and receive mixed data streams over one TCP socket. The receiver looks at the 4-byte `topic_id` in the binary header to demultiplex the byte stream back into separate categories.

### 6. Server-Sent Events (SSE) / Data Streaming
*   **The Concept:** Keeping an HTTP connection open indefinitely to push live data from a server to a client, eliminating the extreme network overhead of continuous "polling".
*   **Where it's Implemented:** `dashboard_server.py`. The proxy server translates the binary TCP packets into JSON and pushes them out through the `/stream` endpoint to the web browser using the HTML5 SSE protocol.

### 7. Durability & Write-Ahead Logging (WAL)
*   **The Concept:** Ensuring network packet durability by sequentially writing data to a non-volatile disk before delivering it to the end consumer.
*   **Where it's Implemented:** `broker/storage.py`. Every raw binary frame is appended to `broker_log.bin` in O(1) time. This allows for your "Time Travel" feature, demonstrating how network nodes can recover lost packets after a crash or disconnection.
