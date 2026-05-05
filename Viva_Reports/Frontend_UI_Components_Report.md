# Frontend UI Components: Role & Behavior Report

This document serves as a comprehensive guide to the Sentinel Home Hub Visualization Dashboard. It breaks down every single component on the screen, detailing what it does, why it is important for your presentation, and exactly how it visually reacts when an action is initiated in the system.

---

## 1. The Header Stats
Located at the top-right of the dashboard, this section provides at-a-glance health metrics for the entire system.
- **Role:** Monitors the live connection status, total messages processed, and session uptime.
- **Importance:** Proves to the evaluator that the frontend is actually connected to a live backend. It shows real-time stability.
- **Reactive Behavior:**
  - **Connection Badge:** If the TCP broker crashes or is stopped, the glowing green dot instantly turns red, and the text changes to "Broker Disconnected." It begins a pulsing animation indicating it is attempting to reconnect.
  - **Message Count:** Every time an SSE event arrives, this number ticks upward instantly, providing a satisfying sense of scale during stress tests.

---

## 2. Live Topology Canvas
The large graphic dominating the center-left of the screen.
- **Role:** An HTML5 `<canvas>` that visually maps the network architecture: Publishers (Sensors) on the left, the Broker in the center, and Subscribers (Apps) on the right.
- **Importance:** It grounds abstract networking concepts into a physical map. Evaluators can actually *see* the fan-out architecture of Pub/Sub routing.
- **Reactive Behavior:**
  - **Idle State:** The canvas slowly pulses with a subtle glow around the nodes to indicate system readiness.
  - **On Action (Simulate Device):** When a message is sent, a highly optimized `requestAnimationFrame` loop kicks in. A glowing colored particle spawns at the specific sensor node (e.g., Red for Fire), traces a bezier curve to the central Broker node, and triggers a bright glow effect on the Broker. A split-second later, multiple particles fan out simultaneously from the Broker to all connected Subscriber nodes.

---

## 3. Live Throughput Panel
Located at the top of the right sidebar.
- **Role:** Displays the current `messages/second` rate and renders a live, trailing line graph of recent network activity.
- **Importance:** Proves the broker's performance capabilities. It visualizes the sheer volume of data the asynchronous TCP server is handling.
- **Reactive Behavior:**
  - Normally sits near zero.
  - **On Action (QoS Burst or Simulator script):** The giant numeric value spikes wildly. The line chart instantly redraws, drawing a sharp peak that slowly travels to the left as time passes, leaving a visual trail of the recent stress test.

---

## 4. Priority Queue Feed
Located directly under the Throughput panel.
- **Role:** A specialized, filtered feed that specifically highlights the QoS (Quality of Service) sorting algorithm.
- **Importance:** This is the most crucial UI element for proving the networking logic works. It proves your backend Min-Heap router is correctly prioritizing critical traffic.
- **Reactive Behavior:**
  - **On Action (Simulation):** When messages arrive, they are dynamically inserted into this feed. 
  - If a low-priority message (Battery) arrives, it gets a subdued green badge.
  - If a high-priority message (Fire) arrives, it slides in with an aggressive red background. During a "QoS Burst", the user watches all the red badges stack up at the very top of the list instantly, clearly jumping ahead of the green badges.

---

## 5. Simulate Devices (Control Grid)
The interactive buttons in the right sidebar.
- **Role:** Allows the user to act as different IoT sensors injecting data into the network.
- **Importance:** Makes the presentation interactive and proves the broker handles diverse payloads and topic hashes.
- **Reactive Behavior:**
  - Clicking a button triggers a fluid, hardware-accelerated CSS depression animation (`transform: scale(0.95)`). 
  - A hidden radial gradient flares up behind the button, providing premium tactile feedback. It disables momentarily to prevent spamming the HTTP proxy, then fires an AJAX POST request into the backend.

---

## 6. Time-Travel Controls
Located at the bottom of the sidebar.
- **Role:** Interfaces with the broker's immutable, append-only binary disk log. Allows the user to select a specific byte offset and request a replay.
- **Importance:** Proves the implementation of the "Event Sourcing" pattern. It demonstrates that the broker isn't just dropping messages, but persisting them securely to disk before routing them.
- **Reactive Behavior:**
  - Dragging the slider dynamically updates the byte offset number.
  - **On Action (Click Replay):** The button turns transparent and says "Replaying…". The UI instantly floods with historical messages. These messages are specifically marked with dashed borders and lower opacity in the UI, visually distinguishing "history" from "live" data.

---

## 7. Protocol Inspector
The black terminal-like box in the bottom-left.
- **Role:** Takes the raw binary payload received over TCP and renders it as a Hexadecimal string, breaking it down into `CMD`, `TOPIC`, `PRI`, and `LENGTH` fields.
- **Importance:** Evaluators in a Computer Networks course want to see packets. This box proves you didn't just use standard heavy JSON over HTTP for your broker. It proves you manually packed bytes (`struct.pack`) into a hyper-efficient 10-byte custom protocol, drastically reducing bandwidth.
- **Reactive Behavior:**
  - **On Action (Simulation):** The moment a live message hits the UI, this box flashes and updates with the exact Hex sequence that traveled across the wire. It calculates the byte savings in real-time (e.g., "70% smaller than JSON").

---

## 8. Live Message Feed
The scrolling feed at the very bottom-right.
- **Role:** The raw activity log of every single event that reaches the dashboard.
- **Importance:** Serves as the ultimate source of truth for the entire system, showing timestamps, exact topic strings, and decoded payloads.
- **Reactive Behavior:**
  - **On Action:** New messages slide down from the top using a custom `@keyframes` CSS animation. The list is automatically culled to keep the browser memory lightweight, perfectly syncing with the canvas and throughput components without any blocking or stuttering.
