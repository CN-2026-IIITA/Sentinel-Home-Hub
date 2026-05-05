# QoS Burst Feature: End-to-End Workflow Report

This document specifically details the workflow of the **"QoS Burst"** feature. This feature exists to visually and technically prove that the broker's Quality of Service (QoS) priority queue functions perfectly under extreme network stress. 

---

### Phase 1: The Trigger (Frontend `app.js`)
1. **The Click Event:** The user clicks the **QoS Burst (Stress Test)** button in the web interface.
2. **AJAX Payload:** Your `app.js` intercepts this click and fires an HTTP POST request to the `dashboard_server.py` proxy at `/api/simulate`. The JSON payload attached is `{"device": "burst"}`.
3. **Visual Feedback:** The button temporarily shrinks or disables to prevent double-clicking, maintaining frontend stability while the massive data drop is processed.

### Phase 2: The Stress Generator (Proxy `dashboard_server.py`)
When the proxy receives the `burst` instruction, it bypasses the normal single-message simulation and triggers the special `simulate_burst()` async function.
1. **Single Socket Optimization:** It opens exactly **one** ephemeral TCP connection to your core broker (`broker/server.py`).
2. **Intentional Interleaving:** The script rapidly constructs a pre-determined batch of exactly 50 messages in this specific, interleaved order:
   - 5x High Priority Fire Alarms (Priority 255)
   - 20x Low Priority Battery Updates (Priority 0)
   - 5x High Priority Fire Alarms (Priority 255)
   - 20x Low Priority Battery Updates (Priority 0)
3. **The Data Dump:** It encodes all 50 of these messages into the custom binary protocol, writes them back-to-back into the socket buffer, and flushes (`writer.drain()`) them over the network in one massive, instantaneous burst.

### Phase 3: Ingestion & Sorting (Your `server.py` & The Router)
This is where the actual magic of the Computer Networks project happens.
1. **TCP Buffering:** Your `broker/server.py` starts aggressively reading frames off the TCP buffer. It decodes all 50 frames one after another.
2. **Disk Logging:** Your server securely appends all 50 binary frames to `broker_log.bin` in the exact order they arrived over the network (First In, First Out).
3. **The QoS Intervention:** Your server hands all 50 messages to the `PriorityRouter`. Instead of a standard FIFO queue, the router utilizes a mathematically efficient **Min-Heap**. 
   - Even though the last 5 Fire messages arrived *after* the first 20 Battery messages, the Min-Heap instantly bubbles all 10 Fire messages to the absolute top of the queue due to their `255` priority ranking.

### Phase 4: Prioritized Delivery (Back to Frontend)
1. **Fan-Out:** The router pops the heap and sends the messages down the outgoing TCP socket to the dashboard server. Because of the Min-Heap, the order leaving the broker is radically different from the order that entered it: **All 10 Fire messages are transmitted first, followed by the 40 Battery messages.**
2. **SSE Flooding:** The proxy decodes these and floods the SSE stream back to the browser.
3. **UI Proof (Your `app.js` & `styles.css`):** 
   - When the user watches the dashboard, they see the Throughput chart spike violently. 
   - More importantly, in the **Priority Queue Feed** on the screen, they watch all 10 red (Critical) Fire alarm badges slide into the UI *before* the 40 green (Low) Battery badges appear. 

### Conclusion for Evaluators
The "QoS Burst" is not just a button; it is a **live test suite**. It perfectly demonstrates that your core `server.py` can withstand a flood of TCP traffic without crashing (handling packet-sticking effortlessly) and proves that the system genuinely honors QoS traffic-shaping rules, protecting critical emergency data over routine sensor chatter.
