# Round-Trip Data Flow: UI to Broker and Back

When you click a simulation button on the frontend, the data makes a lightning-fast round trip through three different layers. 

Here is a visual map of exactly how that information flows:

```mermaid
sequenceDiagram
    autonumber
    participant Browser as Web Browser (app.js)
    participant Dashboard as Dashboard Proxy (dashboard_server.py)
    participant Broker as TCP Broker (main.py)

    Note over Browser, Broker: ⬆️ OUTBOUND PATH (Sending)

    Browser->>Dashboard: HTTP POST /api/simulate (device: "fire")
    Note over Dashboard: 1. Look up config (Priority: 255)<br/>2. Encode into 10-byte Binary Frame
    Dashboard->>Broker: Transmit Binary Bytes over TCP Socket
    
    Note over Broker: 1. Append to disk (WAL Log)<br/>2. Push into Min-Heap Queue<br/>3. Retrieve from Queue
    
    Note over Browser, Broker: ⬇️ INBOUND PATH (Receiving)

    Broker->>Dashboard: Broadcast Binary Bytes over TCP Socket
    Note over Dashboard: 1. Decode Binary Frame<br/>2. Convert payload to JSON string
    Dashboard-->>Browser: Push JSON via Server-Sent Events (SSE)
    Note over Browser: 1. Receive SSE Event<br/>2. Trigger particle animations!
```

### Key Takeaway
Notice how the **Dashboard Proxy** is the middleman protecting the browser. The browser only ever speaks modern web protocols (HTTP/JSON/SSE). The Dashboard Proxy handles the translation to and from your hardcore, custom binary TCP protocol on the fly!
