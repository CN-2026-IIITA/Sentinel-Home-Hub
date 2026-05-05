# Component Deep-Dive: The Protocol Inspector

The **Protocol Inspector** (the black terminal-like box in the bottom left of the visualization dashboard) is arguably the most technical and impressive part of the frontend UI. It exists for one specific reason: **to visually prove to evaluators that a custom binary network protocol was built from scratch.**

In a standard web project, data is sent as large, heavy JSON text files. In an embedded IoT system (like a smart home hub), bandwidth is strictly limited, so data must be sent as raw, optimized bytes. The Protocol Inspector takes those invisible raw bytes traveling over the TCP connection and visualizes them on the screen.

Here is a deep dive into everything it does and how to explain it:

---

### 1. What You Are Actually Seeing (The Hex String)
When you click a device simulation button (like "Temperature"), the UI displays a string of letters and numbers such as `02 a4 b1 09 ff 00 00 00 0b 74 65...`.

This is called **Hexadecimal** (Hex). It is a human-readable way to display raw binary bytes (1s and 0s). 
* Every **2 characters** you see in that box represents exactly **1 Byte** of data traveling over your TCP network.

---

### 2. How the Inspector Breaks Down the Protocol
The UI automatically highlights and splits the Hex string to prove the exact 10-byte structure defined in the `broker/protocol.py` file. It proves that the data isn't just a random stream, but a rigidly structured packet:

* **`[CMD]` (1 Byte):** The very first two hex characters. Usually `02`, which is the hex code for `CMD_PUBLISH`.
* **`[TOPIC HASH]` (4 Bytes):** The next 8 characters. Instead of sending the heavy, variable-length string `"home/temperature"`, the inspector proves the system hashed the string into a tiny 4-byte integer using FNV-1a hashing.
* **`[PRIORITY]` (1 Byte):** The next 2 characters. If you click "Fire", this shows `ff` (which is Hex for 255). If you click "Battery", this shows `00` (Hex for 0). This proves the QoS marker is embedded directly in the network header, not calculated after the fact.
* **`[LENGTH]` (4 Bytes):** The next 8 characters. This tells the broker exactly how many bytes the payload is, preventing buffer overflow attacks and packet-sticking on the TCP layer.
* **`[PAYLOAD]` (Variable Bytes):** The rest of the hex string is the actual sensor data (e.g., `"temp=22.4°C"`) converted into raw UTF-8 bytes.

---

### 3. The "Savings" Metric (The Ultimate Proof)
Below the hex string, the UI renders a dynamic badge that says something like **"22 Bytes (70% smaller than JSON)"**. 

This is the "killer feature" for a Computer Networks presentation. Here is what the frontend Javascript is doing under the hood to calculate this metric:
1. It counts the number of bytes in your custom binary packet (e.g., 22 bytes).
2. It generates a dummy JSON string representing what that *would* have looked like if you had built a normal, lazy web app (e.g., `{"topic":"home/temperature","priority":50,"payload":"temp=22.4°C"}`).
3. It counts the physical byte size of that bloated JSON string (e.g., 68 bytes).
4. It compares them and displays the exact bandwidth savings percentage in real-time.

---

### Presentation Strategy (Viva Talking Points)

When an evaluator asks: 
> *"Why did you use Python's `struct.pack` and a custom protocol instead of just sending JSON over WebSockets like a normal web application?"*

You can trigger a message, point directly to the Protocol Inspector, and answer:
> *"Because JSON is too heavy for IoT networks. As you can see in our Protocol Inspector, our entire packet—including the command, the hashed topic, the priority routing data, and the payload itself—fits into just 22 bytes on the wire. Sending that exact same data in JSON would take over 68 bytes. By engineering a custom binary TCP protocol, we reduced network bandwidth consumption by 70%, which is absolutely critical for preserving battery life and minimizing latency on constrained smart home sensors."*
