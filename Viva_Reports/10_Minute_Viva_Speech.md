# 🎙️ Sentinel Home Hub: 10-Minute Viva Speech Script

**Estimated Time:** 8–10 Minutes
**Pacing:** Speak confidently. Pause at the bolded words for emphasis.

---

### 1. The Introduction & Problem Statement (1 Minute)
"Good Morning Professors. Our project is the **Sentinel Home Hub**—a high-performance, low-latency message broker designed specifically for critical Smart Home IoT networks. 

When researching standard IoT architectures, we noticed a major flaw: most systems rely on HTTP or REST APIs. These protocols are incredibly bloated. They send massive headers and use generic FIFO (First-In, First-Out) queues. If a network is congested with low-priority updates—like a battery sensor pinging every second—a critical life-safety alert, like a **Fire Alarm**, gets stuck at the back of the line.

Our solution was to build an asynchronous Pub/Sub broker from **absolute first principles**. We abandoned high-level web frameworks and dropped directly down to the Transport Layer to fix these bottlenecks."

---

### 2. How Our Project is Different (2 Minutes)
"What makes our project novel is that we did not use an off-the-shelf broker like RabbitMQ or standard MQTT libraries. We engineered the entire networking stack ourselves.

First, we designed a **Custom Application-Layer Protocol**. Instead of sending heavy JSON text over the wire, we engineered a tightly packed **10-byte binary header**. This drastically reduces bandwidth consumption and parsing latency. 

Second, our primary novelty is our **Quality of Service (QoS) Router**. Standard network queues are FIFO. Ours is a mathematically optimized **Min-Heap Priority Queue**. We built a traffic-shaping algorithm that guarantees emergency packets will literally jump to the front of the line, completely bypassing low-priority background noise during extreme network congestion."

---

### 3. Core Computer Network Concepts Implemented (4 Minutes)
"To achieve this, we applied several core concepts from our Computer Networks curriculum directly into our codebase:

**1. Transport Layer & Persistent TCP Sockets:** 
Instead of opening a new connection for every message—which requires an expensive TCP 3-way handshake—our clients open a **single, persistent TCP socket**. We use `asyncio` to keep these pipes open indefinitely, allowing data to stream instantly.

**2. Hashing & Application Protocol Design:**
In our `protocol.py` file, we optimize bandwidth using cryptography. Instead of transmitting a long string like `"home/temperature"` across the network, we run the string through an **MD5 Hash Algorithm**, crushing it down into a tiny 4-byte integer.

**3. Quality of Service (QoS) via Min-Heap:** 
In `router.py`, we implemented our priority queue using Python's `heapq`. Because it is a Min-Heap, we mathematically negate our priority values. A critical Fire Alarm has a priority of 255. By negating it to -255, it instantly bubbles to the top of the heap in **O(log N)** time.

**4. Multiplexing and Demultiplexing:** 
To prevent 'socket exhaustion' on the operating system, our CLI clients and Web Dashboard do not open 5 sockets to listen to 5 sensors. They **multiplex** their subscriptions over one single physical TCP wire. When the byte stream arrives, the receiver looks at the 4-byte topic ID in the binary header to **demultiplex** the data back into distinct categories.

**5. Write-Ahead Logging for Network Durability:**
Finally, we implemented **Event Sourcing**. Before a packet is ever routed, it is appended to a binary file on the hard drive in **O(1) time**. This ensures absolute durability. If the server crashes, zero packets are lost. This allowed us to build a **'Time Travel'** feature, where late-joining nodes can request a specific byte-offset and seamlessly replay historical network events."

---

### 4. Bridging to the Modern Web (1.5 Minutes)
"While the backend speaks raw binary TCP, we needed a way to visualize this throughput in real-time. Web browsers cannot natively speak custom binary TCP protocols.

To solve this, we built a **Middleware Dashboard Proxy**. This server acts as a bilingual translator. It connects to the TCP Broker, unpacks the binary bytes, converts them into JSON, and pushes them to the browser using **HTML5 Server-Sent Events (SSE)**. 

By using unidirectional SSE instead of heavy WebSockets or continuous HTTP polling, we keep the network overhead near zero, allowing the frontend HTML5 Canvas to render glowing particle animations perfectly in sync with the backend traffic."

---

### 5. Transition to the Live Demo (1.5 Minutes)
"To prove that this architecture works, we have set up a multi-laptop demonstration. 

My laptop is currently running the central Broker and the Dashboard Proxy, listening on all Wi-Fi interfaces. My teammate's laptop is connected solely as a visual client via the web browser. 

I will now start our **Load Simulator** script. This script acts as a hostile network environment. It will blast hundreds of mixed-priority packets over a single socket simultaneously. 

If you look at the Dashboard, you will physically see our Min-Heap algorithm in action. You will watch the Red Fire Alarms instantly bypass the Green Battery updates in the queue, proving our QoS Traffic Shaping is successfully prioritizing life-safety data under extreme stress. 

Thank you. We are now open for questions while the simulation runs."
