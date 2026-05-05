# Anticipated Professor Q&A: The Viva Defense

During your Viva, the professors will try to poke holes in your project to see if you actually understand the underlying Computer Networks concepts, or if you just copied code. 

Here are the **10 hardest, most technical questions** a strict Computer Networks professor will ask you, along with the exact answers you should use to impress them and secure top marks for "Technical Depth".

---

### Q1: "Why did you reinvent the wheel? Why not just use HTTP, JSON, or an existing protocol like MQTT?"
**The Professor's Goal:** Checking if you understand the overhead of standard web protocols in an IoT context.
**Your Answer:** 
> "HTTP and JSON carry massive overhead. A simple JSON payload like `{'temp': 20}` requires HTTP headers, string parsing, and takes dozens of bytes. In a constrained Smart Home IoT network running on batteries, every byte drains power. We built a custom binary TCP protocol using Python's `struct.pack` to compress our entire routing header—Command, Topic Hash, Priority, and Length—into exactly 10 bytes. The Protocol Inspector on our dashboard proves that our custom protocol uses **70% less bandwidth** than an equivalent JSON implementation. We didn't use MQTT because we specifically wanted to engineer the Quality of Service (QoS) Min-Heap routing logic from scratch to prove we understand the internals."

### Q2: "You claim you are using raw TCP sockets. TCP is a continuous stream, not a packet protocol. How do you prevent 'packet sticking'?"
**The Professor's Goal:** Checking if you understand the OSI Transport Layer (Layer 4) and socket buffer issues. *(If you answer this well, you guarantee a top grade).*
**Your Answer:** 
> "That is exactly why we designed a fixed-length header! Because TCP doesn't have message boundaries, multiple messages can blur together in the OS buffer (packet sticking). To solve this, our `read_message()` function strictly reads exactly 10 bytes first. It decodes that 10-byte header to find the exact `Payload Length` integer. Only then does it execute a second read for that exact number of payload bytes. By defining strict byte boundaries at the application layer, we guarantee perfect packet framing even under extreme network load, which we prove using the 'QoS Burst' stress test."

### Q3: "How exactly does your priority routing work? If a Fire Alarm comes in after 100 Battery updates, how does it get delivered first?"
**The Professor's Goal:** Checking if you know Data Structures and Algorithms (DSA) and how they apply to Networking.
**Your Answer:** 
> "Instead of a standard First-In-First-Out (FIFO) queue, our `PriorityRouter` implements a **Min-Heap** data structure. When the TCP server receives a message, it extracts the 1-byte priority integer from the header. Fire Alarms are assigned priority 255; Battery updates are priority 0. When they are dropped into the router, the Min-Heap algorithm instantly bubbles the priority 255 message to the absolute top of the tree in `O(log N)` time. The worker loop always pops from the top of the heap, physically guaranteeing that emergency traffic cuts the line and hits the network interface first."

### Q4: "I see your 'Time Travel' feature replays history. Are you querying an SQL database for this?"
**The Professor's Goal:** Checking your understanding of storage architecture and performance bottlenecks.
**Your Answer:** 
> "No, an SQL database would be far too slow for a high-throughput broker and would require parsing our binary frames into tables. Instead, we used an architectural pattern called **Event Sourcing**. Our `storage.py` takes the raw binary packet directly from the TCP socket and simply appends it to a flat file on the hard drive (`broker_log.bin`). Because it is an 'Append-Only' log, disk I/O is incredibly fast. When we Time-Travel, the server just seeks to a specific byte-offset in the file and streams the raw bytes straight back out to the TCP socket without any data translation overhead."

### Q5: "If a sensor sends a topic like 'home/living_room/temperature', doesn't parsing that long string waste broker CPU cycles and bandwidth?"
**The Professor's Goal:** Seeing if you optimized the system.
**Your Answer:** 
> "Yes, string parsing is expensive, which is why we don't do it! Before the sensor (or proxy) sends a message over TCP, it runs the topic string through a hashing algorithm (FNV-1a / MD5). This converts the long string into a tiny, uniform **4-byte unsigned integer**. This integer is what is packed into our 10-byte header. The core broker never sees strings; it only performs blazing-fast integer lookups in its subscription registry. This saves bandwidth on the wire and CPU cycles during routing."

### Q6: "Is this system actually concurrent? If 500 sensors connect at once, doesn't your Python server spawn 500 threads and crash?"
**The Professor's Goal:** Checking your understanding of multi-threading vs asynchronous event loops.
**Your Answer:** 
> "No, our broker is entirely single-threaded. Spawning a thread for every TCP connection would cause massive context-switching overhead and memory exhaustion. Instead, we built the core using Python's `asyncio` event loop, utilizing non-blocking sockets. This is the same architecture Node.js uses. The event loop multiplexes thousands of idle TCP connections concurrently on a single CPU thread, only waking up a task when bytes actually arrive on a specific socket. This is why our broker uses almost zero RAM when idle."

### Q7: "What happens if a sensor goes rogue or gets hacked and tries to send 5 Gigabytes of garbage data to your broker?"
**The Professor's Goal:** Checking if you implemented security and stability safeguards.
**Your Answer:** 
> "We implemented strict Application-Layer safeguards against memory exhaustion attacks. When decoding the 10-byte header, our `server.py` reads the payload length. If that length exceeds our `MAX_PAYLOAD_BYTES` constant (e.g., 5MB), the broker intentionally refuses to read the payload and aggressively terminates the TCP connection. This guarantees that a single malicious IoT device cannot crash the broker by consuming all available RAM (an OOM attack)."

### Q8: "If Subscriber A is on a fast Gigabit network, but Subscriber B is on a terrible 3G connection and reads data very slowly, will Subscriber B block the Fire Alarm from reaching Subscriber A?"
**The Professor's Goal:** Checking your understanding of asynchronous I/O and blocking operations.
**Your Answer:** 
> "No, Subscriber B will not block Subscriber A. Because we use non-blocking `asyncio.StreamWriter` sockets, the broker does not pause its execution to wait for a slow client's network buffer to drain. The broker simply pushes the bytes into the operating system's outgoing network buffer for that specific socket and immediately moves on to the next subscriber. If Subscriber B's buffer fills up completely, the OS handles the backpressure independently of our broker logic."

### Q9: "Your dashboard updates instantly. Are you using short-polling (AJAX) or WebSockets to get that data from the broker to the browser?"
**The Professor's Goal:** Checking your knowledge of modern web communication protocols.
**Your Answer:** 
> "Neither. Polling wastes too many HTTP requests, and WebSockets require complex bidirectional handshakes which we didn't need since the dashboard primarily acts as a receiver. Instead, we used **Server-Sent Events (SSE)**. The `dashboard_server.py` opens a single, long-lived HTTP connection to the browser with the `text/event-stream` header. The proxy translates the raw TCP binary data into JSON and pushes it down this one-way pipe. The browser's Javascript captures it instantly without ever needing to refresh the page."

### Q10: "Is your system locked to Python? What if I wanted to build a Smart Thermostat in C++ or Rust and connect it to your broker?"
**The Professor's Goal:** Seeing if you understand language-agnostic protocol design.
**Your Answer:** 
> "It is completely language-agnostic. Because we operate at the raw TCP socket layer, any programming language that can open a TCP socket and pack bytes according to our strict 10-byte rule can connect to it. A C++ thermostat just needs to use standard POSIX sockets and `htons()`/`htonl()` network byte-order functions to pack the header. The broker has absolutely no idea that the sensor is written in C++ instead of Python; it only sees the bytes."
