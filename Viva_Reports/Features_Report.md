# 🌟 Sentinel Home Hub: Core Features Report

The Sentinel Home Hub is not just a standard web application; it is a high-performance, low-latency messaging architecture built from first principles. Here are the standout technical features that make this project impressive for a Viva presentation:

### 1. Custom Binary Protocol (Zero-Bloat)
Instead of relying on heavy HTTP headers or verbose JSON strings for network transfer, the system uses a custom, tightly-packed **10-byte binary header**. This minimizes bandwidth usage, drastically reduces parsing latency, and proves a deep understanding of transport-layer networking.

### 2. Min-Heap Quality of Service (QoS) Routing
When the network is congested, all messages are not treated equally. The broker utilizes an asynchronous **Min-Heap Priority Queue**. This mathematically guarantees that a Priority 255 `home/fire` alarm will instantly bypass a queue of Priority 0 `home/battery` updates, ensuring emergency data is delivered first.

### 3. Event Sourcing & Write-Ahead Logging (WAL)
Every single binary frame is appended to a persistent disk file (`broker_log.bin`) in **O(1) time** *before* it is routed to subscribers. This architecture guarantees absolute durability—if the power cuts out, zero data is lost. 

### 4. Time-Travel Replay
Because of the Event Sourced architecture, the system supports "Time Travel". A subscriber can request a specific byte offset, and the broker will flawlessly replay all historical binary frames from the disk before seamlessly transitioning the client to a live data stream.

### 5. Multi-Topic, Single-Socket Subscriptions
The CLI `client.py` and the Dashboard Server are both capable of subscribing to multiple distinct topics (e.g., `home/fire,home/door`) multiplexed over a **single, persistent TCP socket**, preventing socket exhaustion on the host machine.

### 6. Zero-Dependency Real-Time Dashboard
The web dashboard operates without React, WebSockets, or third-party bloated libraries. It bridges the binary TCP broker to the browser using pure **HTML5 Server-Sent Events (SSE)**, creating a lightweight, unidirectional "push" stream for live updates.

### 7. Live Network Topology Visualization
The frontend utilizes a highly optimized HTML5 Canvas to render a living map of the network architecture. As binary frames hit the broker, JavaScript spawns glowing particles that physically travel across the screen to the connected subscribers, providing instant visual feedback of backend throughput.

### 8. Interactive CLI & Stress Testing
The system ships with advanced developer tooling:
*   **Interactive Publisher:** Allows a presenter to type commands in the terminal and instantly send them over the TCP stream.
*   **QoS Burst Simulator:** Blasts hundreds of concurrent, mixed-priority messages over a single TCP connection specifically designed to prove the Min-Heap sorting algorithms work under extreme stress.
