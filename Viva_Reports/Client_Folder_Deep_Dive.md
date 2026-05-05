# Client Folder Deep Dive

The `client` directory contains tools for testing and interacting with the core broker. It consists of both active CLI testing utilities and a legacy web dashboard implementation.

## 1. `client.py` (Active Testing Utility)
This is the **primary command-line interface (CLI)** for testing the Pub/Sub broker directly over TCP, without needing a web browser.

**Key Modes of Operation:**
*   **`subscribe`**: Connects to the broker, sends a `CMD_SUBSCRIBE` frame for one or more topics, and listens indefinitely, printing incoming binary frames to the console.
*   **`publish`**: Connects, sends a single `CMD_PUBLISH` frame with a given priority and payload, and immediately disconnects.
*   **`publish-interactive`**: Opens a persistent TCP connection and waits for `stdin` input. Every line typed in the terminal is encoded and sent to the broker instantly.
*   **`time-travel`**: Connects, sends a `CMD_TIME_TRAVEL` frame with a specific byte offset, receives all historical messages from that point, and then seamlessly transitions into a live subscriber.

*Note: This script is essential for the Viva presentation to demonstrate the raw binary protocol working behind the scenes.*

---

## 2. `dashboard.py` (Legacy Web Bridge)
This is an older, lightweight HTTP server and Server-Sent Events (SSE) proxy.

**What it does:**
*   It runs a simple Python `asyncio` HTTP server.
*   It connects to the main broker over TCP as a client.
*   When a web browser connects via `GET /events?topic=...`, it opens an SSE connection and streams broker messages to the browser.
*   It handles `POST /api/publish` to allow the web UI to send messages back to the broker.

**Status: Legacy**
This file has been superseded by `dashboard_server.py` in the root directory. `dashboard_server.py` handles the new, advanced `visualization` UI, whereas `client/dashboard.py` was specifically built to serve the older `client/index.html`.

---

## 3. `index.html` (Legacy Web UI)
This is the older, single-file HTML/CSS/JS frontend that pairs with `client/dashboard.py`.

**Features:**
*   Basic CSS styling with a dark theme.
*   A simple message feed that displays incoming SSE messages as cards.
*   A manual "Subscribe to Topic" input box.
*   A manual "Publish Message" form.

**Status: Legacy**
It lacks the advanced "Live Topology" canvas animations, the interactive Quality of Service (QoS) burst testing tools, and the real-time throughput metrics that are now available in the modern `visualization/` UI directory.

---

## Conclusion
For your final project submission and Viva:
1.  **Actively Use `client.py`**: It is crucial for terminal demonstrations, running the interactive publisher, and proving the architecture works independent of a web browser.
2.  **Ignore `dashboard.py` & `index.html`**: They are safe to keep in the repository as historical references or simple fallback backups, but you should exclusively use `dashboard_server.py` and the `visualization/` folder for your actual demonstration.
