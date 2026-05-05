# Viva Presentation Strategy: Sentinel Home Hub

This document is your master script for the project evaluation. It strips away the code files and focuses entirely on **system architecture, engineering decisions, and satisfying the exact grading parameters** provided by your professors.

---

## 1. The Elevator Pitch (What it is & Problem Solved)
**What it is:** Sentinel Home Hub is a high-performance, asynchronous TCP Pub/Sub Message Broker built entirely from scratch, specifically tailored for constrained IoT (Internet of Things) environments. 

**The Problem it Solves:** Traditional web applications use HTTP and JSON to communicate. In a Smart Home IoT network with thousands of low-power sensors, sending a 70-byte JSON string just to say "temperature is 20°C" drains batteries rapidly and clogs the network. Furthermore, standard networks treat all traffic equally, meaning a critical Fire Alarm might be delayed behind 1,000 mundane battery updates.

**The Solution:** We built a broker that solves this by implementing a **Custom Binary Protocol** (reducing bandwidth by 70%) and an integrated **Quality of Service (QoS) Min-Heap Router** (guaranteeing emergency traffic always jumps to the front of the line).

---

## 2. Satisfying the Rubric

### Parameter 1: Novelty (Target: 3/3 - Clear originality)
*How to sell it:* If you just say "We built a Pub/Sub broker," they will give you a 1 (Common idea). You must emphasize the **Customizations and Edge-Cases**.
* **Talking Points:**
  * "While Pub/Sub is a known pattern (like MQTT), we did not use existing libraries. We engineered our own binary wire protocol from scratch."
  * "Our novelty lies in the **QoS Traffic Shaping**. Most student projects use basic FIFO (First-In, First-Out) queues. We implemented a mathematically efficient **Min-Heap priority queue** at the routing layer so that life-safety events (Fire Alarms) physically bypass low-priority telemetry."
  * "We also implemented **Event Sourcing (Time-Travel)**. Because we log raw bytes directly to disk before routing, our broker can instantly replay historical network states from arbitrary byte offsets."

### Parameter 2: Functionality (Target: 4/4 - Efficient, handles edge cases)
*How to sell it:* Prove that the system doesn't just work on the "happy path," but survives stress and hostile conditions.
* **Talking Points:**
  * **Edge Case - TCP Packet Sticking:** "A common bug in raw socket programming is packet sticking, where multiple messages arrive in one buffer. We handled this edge case by designing a strict 10-byte header. The server strictly reads exactly 10 bytes first, parses the payload length, and only then reads the payload. This guarantees perfect framing under massive network stress."
  * **Edge Case - OOM (Out of Memory) Attacks:** "We handle malicious/broken sensors by enforcing a `MAX_PAYLOAD_BYTES` limit at the socket layer, preventing a single sensor from crashing the broker."
  * **Performance Proof:** Hit the **QoS Burst** button on the UI during the presentation to visually prove the server easily ingests 50 simultaneous TCP connections without dropping packets.

### Parameter 3: Technical Depth (Target: 3/3 - Protocol design, distributed logic)
*How to sell it:* Lean heavily on your protocol and asynchronous architecture.
* **Talking Points:**
  * **Protocol Design:** Show them the Protocol Inspector on the UI. Explain how you used Python's `struct.pack` to compress the Command, Priority, Payload Length, and Topic into exactly 10 bytes.
  * **Optimization (Topic Hashing):** "Strings are expensive to route. Instead of sending 'home/temperature' over the wire, our proxy runs the string through an MD5/FNV-1a hash algorithm, converting it into a tiny 4-byte integer before it ever hits the TCP socket. The core broker only routes integers."
  * **Distributed Architecture:** Explain that the web dashboard does NOT talk to the broker directly. It uses an API Gateway (Proxy Server) that bridges HTTP/SSE to the raw TCP broker, demonstrating modern decoupled microservice architecture.

---

## 3. Limitations & Future Scope (Crucial for Top Grades)
*Professors love when students critically analyze their own work. Mentioning these shows maturity.*
* **Security (Limitation):** "Currently, the binary protocol is unencrypted. Anyone wiretapping the TCP port can read the payload. In the future, we would implement a TLS handshake over the socket."
* **Topic Collisions (Limitation):** "Because we hash variable-length topics into 4-byte integers to save bandwidth, there is a mathematical possibility of a hash collision (two different topics generating the same ID). Given our smart home scope, the risk is incredibly low, but an enterprise version would use an 8-byte hash."
* **Clustering (Future Scope):** "Right now, it is a single-node broker. To achieve true high-availability, we would need to implement the Raft consensus algorithm to cluster multiple broker nodes together."

---

## 4. Anticipated Professor Q&A

**Q: Why didn't you just use WebSockets?**
**A:** "WebSockets are great for browsers, but they carry heavy HTTP framing overhead. We wanted to simulate real, heavily constrained IoT devices (like microcontrollers running on coin-cell batteries). For them, every byte matters, which is why we dropped down to bare TCP and wrote a custom 10-byte protocol."

**Q: How do you prove your QoS (Priority) actually works?**
**A:** "If you look at the visualization dashboard and I hit the 'QoS Burst' button, the proxy sends 50 interleaved messages (10 Fire, 40 Battery) over the network. Because the router uses a Min-Heap, you will visually see all 10 Red Fire alarms jump to the front of the queue and get delivered first, definitively proving the traffic-shaping logic."

**Q: Where is your database? How are you saving data?**
**A:** "We purposefully avoided using a heavy SQL database to maximize throughput. We use an **Append-Only Binary Log** (`broker_log.bin`). The `storage.py` file simply takes the raw network bytes and flushes them directly to the hard drive. This 'Event Sourcing' pattern is what powers our Time-Travel feature, as we just read byte offsets directly from the disk."
