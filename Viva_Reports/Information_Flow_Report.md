# System Architecture: Information Flow Report

This report provides a deep, step-by-step technical analysis of the data lifecycle within the Sentinel Home Hub (Pub/Sub Broker) ecosystem. 

## Phase 1: Ingestion (HTTP → TCP)

This phase covers the journey of a message from the moment a user interacts with the web interface until the raw bytes hit the core broker's network boundary. It highlights the crucial translation from standard web protocols (HTTP/JSON) to the highly optimized, custom binary TCP protocol.

### 1. The User Trigger (Frontend)
The flow initiates in the browser when a user clicks a device simulation button on the Dashboard interface (e.g., the **Fire Alarm** button).
- The `app.js` file captures this `click` event.
- It disables the button momentarily to prevent spam and constructs a standard AJAX request.
- A `POST` request is fired to the `dashboard_server.py` at the `/api/simulate` endpoint, carrying a simple JSON payload: `{"device": "fire"}`.

### 2. API Gateway Processing (Proxy Server)
The `dashboard_server.py` is listening for HTTP traffic on port `8080`. When the `POST` request arrives:
- The async `handle_http_request` function reads the request headers and extracts the `Content-Length`.
- It reads the JSON body and extracts the requested device type.
- It looks up the specific device characteristics from its internal dictionary (`DEVICE_CONFIGS`). For a "fire" device, it retrieves the predefined topic (`home/fire`), the critical QoS priority (`255`), and the specific string payload (`"🔥 FIRE DETECTED in sector 7G!"`).

### 3. Binary Protocol Encoding
Because the core broker is designed for maximum throughput and minimalistic IoT environments, it *does not understand HTTP or JSON*. It only speaks a custom binary protocol. The proxy server must translate the data.
- The proxy imports the `encode` function from `broker.protocol.py`.
- It hashes the topic string (`home/fire`) into a compact 4-byte integer using Fowler–Noll–Vo (FNV-1a) hashing. This prevents the broker from having to parse expensive variable-length strings during routing.
- The `encode` function uses Python's `struct.pack("!B I B I", ...)` to tightly pack the data into a strict 10-byte header:
  - **1 Byte:** Command (`0x02` for PUBLISH)
  - **4 Bytes:** Topic Hash ID
  - **1 Byte:** Priority (`255`)
  - **4 Bytes:** Payload Length
- The string payload is encoded to raw UTF-8 bytes and appended to the header, forming a single, continuous binary frame.

### 4. Ephemeral TCP Transmission
With the binary frame constructed, the dashboard proxy now acts exactly like a tiny, standalone IoT device on the network.
- It invokes `asyncio.open_connection(BROKER_HOST, BROKER_PORT)` to establish an asynchronous, ephemeral TCP socket connection directly to the core broker on port `9999`.
- It streams the raw bytes over the TCP connection using `writer.write(frame)` and awaits `writer.drain()` to ensure the OS network buffers have fully flushed the data.
- Once transmitted, the proxy purposefully closes the TCP connection (`writer.close()`). This stateless behavior perfectly mimics a constrained IoT sensor waking up, firing off a reading, and going back to sleep to save battery.

---

## Phase 2: The Core Network Layer (`server.py`)

This phase focuses entirely on your specific domain: the high-performance, asynchronous TCP server that acts as the heart of the broker. It handles the raw network sockets, prevents data corruption, ensures persistence, and bridges the gap to the router.

### 1. Connection Acceptance & Registration
When the proxy's ephemeral TCP connection reaches the operating system, your `server.py` code springs into action.
- The `BrokerServer` runs an `asyncio.start_server` loop perpetually listening on port `9999`. 
- When the connection arrives, `_handle_client(reader, writer)` is immediately spawned as a lightweight, concurrent async task.
- A unique `client_id` (a 12-character UUID) is generated for the connection.
- The connection is registered with your `ConnectionRegistry`. The registry securely maps the `client_id` to the socket's `StreamReader` and `StreamWriter`. This exact architecture is what allows your broker to manage thousands of concurrent connections efficiently without thread-blocking.

### 2. Binary Decoding & Frame Validation
With the socket open, your server begins reading from the stream. This step is critical for network stability.
- Inside the client loop, `server.py` invokes `read_message(reader)`.
- It uses `reader.readexactly(10)` to pull exactly 10 bytes from the TCP stream. This guarantees it reads a perfect header, effortlessly preventing "TCP packet sticking" (a common network issue where multiple frames blur together in the buffer).
- The `struct.unpack` command rips the 10-byte header apart into its constituent variables (`command`, `topic_id`, `priority`, `length`).
- If the `length` is > 0, it calls `readexactly(length)` to capture the exact payload.
- The server validates the packet size against `MAX_PAYLOAD_BYTES` to protect the broker from memory exhaustion attacks (OOM crashes) caused by rogue or malicious IoT devices sending gigabytes of garbage data.

### 3. Event Sourcing (Disk Logging)
Because the decoded command from Phase 1 is `CMD_PUBLISH`, your server routes the logic to `_handle_publish()`.
- **Crucial Step:** Before *any* routing or fan-out occurs, your server takes the raw binary frame and sends it to `self.event_log.append(frame)` inside `storage.py`.
- The bytes are flushed directly to the physical disk (`broker_log.bin`), creating an immutable, append-only ledger.
- The storage layer returns the precise byte `offset` of where this message was written. Your server updates its internal state (`self.latest_offset`) and keeps track of `messages_processed`. 
- By saving to disk *first*, you guarantee no data loss even if the broker crashes milliseconds later. Furthermore, tracking this exact byte `offset` is what natively powers the "Time Travel" history replay functionality.

### 4. QoS Hand-off & Disconnect
With the message safely persisted, your server prepares it for delivery.
- `server.py` wraps the decoded data into a clean internal Python `Message` dataclass.
- It hands this object off to the Quality of Service layer by calling `self.router.enqueue(msg)`. 
- At this precise moment, your `server.py`'s job for this specific incoming connection is complete. The ephemeral TCP socket from the proxy is gracefully closed and cleaned up via the `finally: await self.registry.unregister(client_id)` block, freeing up memory and file descriptors for the next incoming sensor connection.

---

## Phase 3: Fan-Out Delivery (`server.py` & Router)

Once the message leaves the ingestion loop, it enters the routing domain. This phase demonstrates how your server code coordinates with the Quality of Service layer to distribute the message back out to active clients.

### 1. Priority Resolution (QoS)
- The message sits in the `PriorityRouter`'s internal min-heap. Because it is a Fire Alarm (priority 255), it immediately jumps to the absolute front of the line, bypassing any accumulated low-priority messages (like battery updates).
- The router's asynchronous worker loop pops this high-priority message off the heap.

### 2. Subscriber Lookup
- The router takes the message's `topic_id` and asks your `ConnectionRegistry` (managed by `server.py`) for a list of all active TCP connections that have explicitly requested to hear about this topic.
- `self.registry.get_subscribers(topic_id)` returns a list of matching `StreamWriter` socket objects. 
- *Note:* One of these subscriber sockets is a permanent, long-lived TCP connection maintained entirely by the dashboard proxy server, which subscribed to all topics upon boot.

### 3. Binary Re-encoding & Transmission
- For every matching subscriber, the server re-encodes the `Message` dataclass back into the raw, 10-byte binary frame using `encode()`.
- It executes `writer.write(frame)` and `writer.drain()` to shoot the raw bytes down the outbound TCP socket to the waiting dashboard server.

---

## Phase 4: The Bridge (TCP → SSE)

This phase shows how the raw binary data leaving your TCP broker is translated back into web-friendly formats to power the live visualization.

### 1. Persistent Stream Consumption
- The `dashboard_server.py` has a background async loop (`proxy_broker_connection()`) that permanently listens to its open TCP socket from the broker.
- It calls `_read_one(reader)`, mimicking your broker's exact logic to perfectly slice the incoming binary stream into isolated 10-byte frames.

### 2. Protocol Translation (Binary to JSON)
- The proxy decodes the binary header to extract the original `topic_id`, `priority`, and raw payload bytes.
- It converts the `topic_id` hash back into a human-readable string (e.g., `"home/fire"`).
- It additionally takes the entire raw binary string and converts it to a Hexadecimal string (so it can be displayed in your UI's Protocol Inspector).
- It packages all of this data into a standard Python dictionary: `{"type": "qos", "topic": "home/fire", "priority": 255, "payload": "...", "hex": "..."}`.

### 3. Server-Sent Events (SSE) Pipeline
- The proxy executes `broadcast_sse(data)`, converting the Python dictionary into a JSON string.
- This JSON string is pushed into an `asyncio.Queue` belonging to the connected web browser.
- The `/stream` HTTP endpoint pulls from this queue and flushes it out to the browser using the `text/event-stream` format (`data: {...}\n\n`).

---

## Phase 5: Live UI Update (`app.js` & `styles.css`)

This final phase represents your second major domain of responsibility: The Visualization Frontend. Here, the browser catches the SSE event and flawlessly renders the massive influx of data using hardware-accelerated animations and modern CSS.

### 1. Non-Blocking Event Listener
- The `app.js` file maintains a perpetual `EventSource('/stream')` connection to the proxy server.
- The browser triggers the `onmessage` callback the exact millisecond the new JSON data arrives. Because this utilizes SSE rather than short-polling, network overhead is effectively zero, providing pure real-time feedback.

### 2. Canvas Particle Animation
- `app.js` parses the JSON and looks at the priority (`255`) and the topic (`home/fire`).
- It extracts the precise color token associated with fire (Rose red: `#f43f5e`) from the hardcoded JS `COLORS` dictionary.
- It creates a new `Particle` object in the HTML5 Canvas system.
- The `requestAnimationFrame(animate)` loop smoothly updates the particle's X/Y coordinates 60 times a second. The particle calculates a bezier curve to travel visually from the "Publisher" node, to the "Broker" node, and out to the "Subscriber" nodes. The particle's size and speed are dynamically calculated based on its QoS priority (higher priority = faster and larger).

### 3. Dynamic DOM Manipulation
Simultaneously, `app.js` mutates the DOM to reflect the incoming data:
- **Priority Queue Feed:** It calls `addToQueue()`, which unshifts the new message into the queue UI. It applies the specific red CSS classes to highlight the message's critical nature.
- **Protocol Inspector:** It writes the raw Hex bytes into the `<pre class="hex-display">` element, parsing out the CMD, TOPIC, and PAYLOAD bytes so an evaluator can visually verify the broker's underlying binary protocol efficiency.
- **Throughput Metrics:** The line chart `canvas` natively re-draws its graph to visualize the live messages/second rate.

### 4. Modern Glassmorphism Styling (`styles.css`)
- As the DOM updates, your `styles.css` rules instantly apply modern visual design paradigms.
- The inserted feed messages trigger CSS `@keyframes` animations (`feedSlide` and `queuePop`) to smoothly slide and pop into place.
- The UI panels wrap the data in premium Glassmorphism: `backdrop-filter: blur(24px)` combined with subtle glowing drop shadows (`box-shadow: var(--shadow-glass)`) provide a stunning, native-app feel.
- The background pulses utilizing complex radial-gradients and grid overlays to provide a state-of-the-art cybernetic aesthetic, proving that the front-end UX matches the extreme performance of your back-end TCP server.
