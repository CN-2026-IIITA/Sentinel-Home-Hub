# Dashboard Server Deep Dive (`dashboard_server.py`)

In the Sentinel Home Hub architecture, `dashboard_server.py` plays a critical role as the **"Middleware Bridge"** between your raw binary TCP broker and the modern web browser.

It solves a fundamental problem: web browsers cannot natively speak custom binary TCP protocols. They speak HTTP and WebSockets/SSE. The Dashboard Server translates between the two worlds.

## Core Responsibilities

### 1. The Super-Subscriber (TCP Client)
When `dashboard_server.py` starts, the very first thing it does is open a standard TCP connection to your main broker (`main.py` on port 9999).
*   It looks at the `SMART_HOME_TOPICS` dictionary.
*   It automatically sends multiple `CMD_SUBSCRIBE` binary frames to the broker, subscribing itself to **all 6 topics** simultaneously.
*   It then loops infinitely, reading incoming binary frames and decoding them back into topic IDs, priorities, and payloads.

### 2. Server-Sent Events (SSE) Proxy
Every time the TCP Client layer receives a binary message from the broker, it cannot simply hand raw bytes to the browser.
*   It converts the data into a JSON string.
*   It looks at its list of connected `sse_clients` (the open web browser tabs).
*   It pushes that JSON string into the event queue for every connected browser using **HTML5 Server-Sent Events (SSE)**.
*   This creates a lightweight, unidirectional "push" stream that allows the browser to receive live updates with near-zero latency, avoiding the heavy overhead of WebSockets.

### 3. HTTP Web Server
In addition to handling the live data stream, `dashboard_server.py` acts as a traditional web server hosting port `8080`.
*   It serves the static files (`index.html`, `app.js`, `styles.css`) located inside your `/visualization` directory.
*   When you navigate to `http://localhost:8080`, this server reads those files from your hard drive and sends them to your browser.

### 4. RESTful API Endpoints (The Control Plane)
The web browser needs a way to send commands *back* to the broker (e.g., when you click a button). The Dashboard Server exposes HTTP endpoints to handle this:
*   **`POST /api/publish`**: Takes a JSON request from the browser, encodes it into a raw binary frame, and writes it directly to the TCP socket connected to the broker.
*   **`POST /api/simulate`**: This is where the magic behind the UI buttons happens. If the browser requests `device: "fire"`, the server automatically generates the correct priority (255) and payload, encodes it, and sends it to the broker. If the browser requests `device: "burst"`, it triggers the `simulate_burst()` function to blast 50 mixed messages for the QoS test.
*   **`POST /api/timetravel`**: Takes a byte offset from the browser, encodes a `CMD_TIME_TRAVEL` binary frame, and requests historical replay from the broker.

## Architectural Summary
To put it simply: `dashboard_server.py` is a bilingual translator. 
On its left side, it speaks raw, high-speed binary bytes over TCP to `main.py`. On its right side, it speaks standard HTTP and JSON to your web browser. 

Without this file, you would only be able to interact with your broker using terminal windows!
