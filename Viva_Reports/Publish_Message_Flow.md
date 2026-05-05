# The Publish Message Lifecycle

When you run a command like `python3 -m client.client publish --topic home/fire --priority 255 --message "FIRE!"`, the message undergoes a fascinating journey through your custom architecture.

Here is the exact step-by-step flow, and the binary form it takes during transfer.

## 1. Client Encoding (The Sender)
Before the message leaves your laptop, `client.py` prepares it. It doesn't send JSON or text over the wire; it uses your custom **Binary Protocol** for maximum efficiency.
*   It hashes the string `"home/fire"` into a 32-bit integer (`topic_id`).
*   It packs the data into a raw byte array (binary frame). 

**The Binary Frame Format (10-byte header + payload):**
```text
[ 1 byte ]  Command Type (0x02 for CMD_PUBLISH)
[ 4 bytes]  Topic ID (Unsigned 32-bit Integer)
[ 1 byte ]  Priority (0-255)
[ 4 bytes]  Payload Length (Unsigned 32-bit Integer)
[ N bytes]  The actual message payload ("FIRE!")
```

## 2. Network Transfer
This raw binary frame is transmitted over a **TCP Socket** directly to the broker (usually listening on port `9999`). 

## 3. Broker Decoding
Inside `broker/server.py`, the `read_message()` function reads the TCP stream.
*   It first reads exactly 10 bytes to get the header.
*   It uses `struct.unpack("!BIBI")` to decode the header.
*   It reads the `Payload Length` and then reads exactly that many remaining bytes from the socket.
*   It packages this into a Python `Message` dataclass.

## 4. Write-Ahead Logging (Durability)
Before the broker tries to send the message to anyone, it calls `self.event_log.append(frame)`. 
This instantly appends the raw binary frame to `broker_log.bin` on your hard drive. This is **Event Sourcing**—if the server crashes immediately after this step, the message is safe on disk and can be recovered later using Time-Travel.

## 5. The Min-Heap Priority Queue (QoS Routing)
The `Message` object is then handed to the `PriorityRouter` (`broker/router.py`).
*   The router places the message into an `asyncio.PriorityQueue` (a Min-Heap data structure).
*   Because it's a *Min*-Heap, the router mathematically inverts the priority (e.g., `255` becomes `-255`) so that high-priority emergency messages bubble to the absolute top of the queue, cutting in line ahead of low-priority temperature updates.

## 6. Fan-out Delivery (The Subscribers)
A background worker continuously pops the most important message off the top of the Min-Heap.
*   It asks the `ConnectionRegistry`: *"Which TCP sockets are subscribed to this topic ID?"*
*   It takes the `Message` object, re-encodes it back into the exact same **10-byte binary frame** format.
*   It loops through every subscribed client (e.g., your terminal subscribers and the `dashboard_server.py` web proxy) and writes the binary frame directly to their TCP sockets.

## Summary
The data transfers purely as **raw, packed binary bytes** over TCP. This avoids the overhead of HTTP headers and JSON parsing, which is exactly why your Sentinel Home Hub is capable of such extreme low-latency performance!
