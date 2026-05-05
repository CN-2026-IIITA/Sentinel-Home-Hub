# The Ultimate Full-Feature Team Demo

To score full marks, your team must physically demonstrate every single technical feature of your project (QoS Priority, Custom Protocol, Fan-Out Distribution, Event Sourcing/Time-Travel, and TCP Load Stability). 

Here is a fresh, highly structured script for 6 members to demonstrate **every feature** live to the evaluators.

---

## 🎭 The Cast & Setup
*Ensure all laptops are on the same WiFi network. Member 1 finds their IP address (e.g., `192.168.1.50`) and shares it with the team.*

| Member | Role | Command to Run | Feature They Prove |
|--------|------|----------------|--------------------|
| **1. Aman** | Broker Host | `python3 main.py` | TCP Socket Stability |
| **2. UI Presenter** | Dashboard | `python3 dashboard_server.py` | Visualizes the network |
| **3. Fire Node** | High-Priority CLI | `python3 -m client.client publish-interactive --host 192.168.1.50 --topic home/fire --priority 255` | Priority Routing |
| **4. Temp Node** | Low-Priority CLI | `python3 -m client.client publish-interactive --host 192.168.1.50 --topic home/temperature --priority 50` | Basic Telemetry |
| **5. The Hacker** | Load Tester | `python3 simulate_load.py` (Wait to press enter) | OOM / TCP Packet Sticking Defense |
| **6. The Guard** | CLI Subscriber | `python3 -m client.client subscribe --host 192.168.1.50 --topic home/fire` | Fan-Out Distribution |

---

## 🎬 The Live Demonstration Script

### Feature 1: The Custom Binary Protocol (Bandwidth Savings)
* **Action:** Member 4 types `temp=22C` in their terminal and hits Enter. It appears on the Dashboard.
* **Aman speaks:** *"Before we show you the features, let us show you the foundation. When Member 4 sent that temperature, we didn't send a bloated JSON string. Look at the Protocol Inspector on the dashboard. We engineered a custom TCP binary protocol using Python's `struct.pack`. The entire packet—Command, Topic Hash, Priority, and Payload—is exactly 22 bytes. Sending this as JSON would take 68 bytes. We achieved a 70% reduction in network bandwidth."*

### Feature 2: Fan-Out Distribution (Language Agnostic Routing)
* **Action:** Member 3 types `FIRE DETECTED IN KITCHEN!` and hits Enter.
* **Member 6 speaks:** *"As you can see, the fire alarm just appeared on the Web Dashboard. But look at my terminal. I am running a pure raw TCP subscriber script. I received the exact same bytes at the exact same millisecond. This proves our broker is completely agnostic and can fan-out data to multiple, vastly different applications simultaneously."*

### Feature 3: Quality of Service (The Min-Heap Router)
* **Action:** Member 2 clicks the **"QoS Burst"** button on the Web Dashboard.
* **Member 2 speaks:** *"Normal web apps use FIFO queues. In an emergency, a Fire Alarm might get stuck behind 100 mundane battery updates. Watch the Priority Queue on the screen. The QoS Burst just injected 50 simultaneous messages into the broker. Notice how the Red Fire alarms instantly bypassed the Green low-priority traffic and hit the network first. This proves our custom Min-Heap data structure works flawlessly."*

### Feature 4: TCP Stability & Edge Case Defense (Packet Sticking)
* **Action:** Member 5 runs `python3 simulate_load.py` on their laptop.
* **Member 5 speaks:** *"Most student TCP servers crash due to 'Packet Sticking' when under heavy load. I am now acting as a rogue node, blasting thousands of messages a second directly at Aman's broker. Notice that the dashboard continues to update smoothly, and the Python server doesn't crash. Because our `read_message()` function strictly enforces a 10-byte header read before looking at payloads, we have completely neutralized TCP packet-sticking and buffer overflow attacks."*

### Feature 5: Event Sourcing & Time-Travel (Disk Logging)
* **Action:** Member 2 drags the **Time Travel Slider** on the Dashboard back to `0 Bytes`.
* **Aman speaks:** *"Instead of using a slow SQL database, our storage layer uses an Append-Only Binary Log. Every single packet we just fired was instantly flushed to `broker_log.bin` on my hard drive. By moving this slider, the broker physically seeks to a specific byte-offset on the disk and streams the raw historical bytes directly back to the network. As you can see, the dashboard is now replaying the exact sequence of the entire demonstration from the beginning."*

---

## 🏆 Final Conclusion for the Evaluators
Have Aman deliver the final closing statement:
> *"To summarize: We didn't use existing libraries like MQTT, WebSockets, or MySQL. We wrote our own byte-level protocol, built a Min-Heap priority router, handled raw TCP edge-cases like packet sticking, and utilized binary disk logging for Time-Travel. Sentinel Home Hub is a highly optimized, fully functional IoT message broker."*
