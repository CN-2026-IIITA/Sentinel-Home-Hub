# Solo Testing Guide: Sentinel Home Hub

If you are testing or presenting this project entirely by yourself, this guide provides a seamless, step-by-step script to demonstrate every major Computer Networks feature in the system without needing a partner.

---

## Preparation: The Multi-Terminal Setup
To demonstrate a distributed system effectively by yourself, you must show multiple independent processes running simultaneously.
1. Open your terminal program and create **three separate tabs or windows**.
2. Navigate to your project directory (`Sentinel-Home-Hub`) in all three tabs.

---

## Test 1: System Bring-Up & Connection Validation
**Goal:** Prove the backend infrastructure boots up and the frontend connects seamlessly.

1. **Start the Core Broker:**
   - In Terminal 1, run: `python3 main.py`
   - *What to say/observe:* Point out the log saying "Broker listening on 127.0.0.1:9999". This proves the async TCP server is ready.
2. **Start the Dashboard Proxy:**
   - In Terminal 2, run: `python3 dashboard_server.py`
   - *What to say/observe:* The proxy immediately logs that it has connected to the broker and subscribed to topics.
3. **Open the UI:**
   - Open your browser to `http://localhost:8080`.
   - *Validation:* Point to the top-right header. The status dot should be green and say "Broker Connected". The canvas should be pulsing. You have proven the HTTP -> TCP -> SSE bridge is alive.

---

## Test 2: Standard Telemetry & Protocol Efficiency
**Goal:** Prove the system can route simple data and that you built a custom, lightweight binary protocol.

1. **Action:** On the web dashboard, click the blue **Temperature** button once.
2. **Visual Proof (Canvas):** A cyan particle travels from the publisher to the broker, then fans out.
3. **Technical Proof (Inspector):** Direct attention to the **Protocol Inspector** (bottom left).
   - Point out the Hex string.
   - Show how the data is perfectly packed into a 10-byte header.
   - Mention the byte savings (e.g., "70% smaller than JSON"), proving you didn't just use standard web protocols for an IoT system.

---

## Test 3: Quality of Service (QoS) & Traffic Shaping
**Goal:** Prove the core broker correctly prioritizes emergency traffic over mundane traffic using your Min-Heap implementation.

1. **Action:** Click the **QoS Burst (Stress Test)** button on the dashboard.
2. **Visual Proof (Throughput):** Watch the Throughput chart spike dramatically, proving the async TCP server handles rapid load without crashing (no packet sticking).
3. **Technical Proof (Queue Feed):** Immediately look at the **Priority Queue** panel.
   - *What to point out:* The burst sent 50 interleaved messages (Fire and Battery). However, in the UI, all the red "Fire Alarm" (Priority 255) badges appear *first* at the top of the feed, while the green "Battery" (Priority 0) badges are forced to the bottom.
   - This physically proves the `PriorityRouter` Min-Heap logic works under network stress.

---

## Test 4: Event Sourcing & Time Travel
**Goal:** Prove the `storage.py` disk logger works and that the broker can replay history from an arbitrary byte offset.

1. **Action:** Let the system sit idle for a moment. Go to the **Time-Travel** panel on the left.
2. **Select Offset:** Drag the slider to the middle (e.g., somewhere around 2KB or 4KB).
3. **Trigger Replay:** Click the **"⏪ Replay History"** button.
4. **Visual Proof:**
   - The UI will suddenly flood with historical messages.
   - Point out that these messages have a lower opacity and dashed borders in the Live Message Feed, proving the UI knows this is historical data.
5. **Technical Proof:** Look at Terminal 1 (`main.py`). You will see logs indicating exactly how many bytes were skipped and how many historical messages were served from the `broker_log.bin` disk file.

---

## Test 5: The CLI Client (Optional, but highly recommended)
**Goal:** Prove your broker is truly application-agnostic and doesn't just work with the web dashboard.

1. **Action:** Go to Terminal 3. Run: `python3 client/client.py subscribe`
2. **Observe:** The terminal will say it is listening to the broker.
3. **Cross-System Test:** Go back to your Web UI and click the **Door** button.
4. **Visual Proof:** 
   - The Web UI will show the door event.
   - *Immediately* look at Terminal 3. The exact same door event will print in the terminal window.
   - *What to say:* "This proves the broker is actively fanning out raw TCP packets to multiple distinct clients written in different technologies simultaneously."

---

## Conclusion of Solo Run
By following these 5 steps, you independently demonstrate:
1. Async TCP Networking
2. Custom Binary Protocols
3. Min-Heap QoS Routing
4. Disk I/O & Event Sourcing
5. WebSockets/SSE Streaming
...all flawlessly working together.
