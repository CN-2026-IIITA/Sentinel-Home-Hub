# 🚀 Sentinel Home Hub: Live Demo Command Cheat Sheet

This guide contains every terminal command you and your team will need during the live Viva presentation. **Keep this open on your screen during the demo.**

> [!IMPORTANT]
> Ensure all team members are in the `/Users/amankashyap/Developer/Sentinel-Home-Hub` directory before running any commands.
> If running across multiple laptops, replace `127.0.0.1` or `--host` arguments with the IP address of the main host laptop.

---

## Phase 1: Booting the Core Infrastructure (Host Laptop)

Start these first before showing anything to the evaluator. Open two separate terminal windows.

**1. Start the TCP Broker (The Core)**
```bash
python3 main.py
```
*Leave this running in the background.*

**2. Start the Web Dashboard Proxy**
```bash
python3 dashboard_server.py
```
*Once running, open `http://localhost:8080` in your web browser.*

---

## Phase 2: Simulating "Normal" Smart Home Activity

Before you explain the architecture, start the normal simulator so the Web Dashboard looks alive and active with realistic data.

**3. Run the Normal Day Simulator**
```bash
python3 smart_home_simulator.py
```
*This will slowly tick temperature changes, open/close doors, drain the battery, and ping the fire alarm status in the background.*

---

## Phase 3: CLI Demonstration (Team Members)

Have your team members open terminals on their laptops (or use multiple tabs on yours) to show the custom CLI working alongside the web dashboard.

**4. The Universal Subscriber (Listening to multiple topics at once)**
```bash
python3 -m client.client subscribe --topic "home/fire,home/door,home/battery"
```
*This will instantly connect and start printing the live binary data that the broker is routing.*

**5. The Interactive Publisher (Sending custom data)**
```bash
python3 -m client.client publish-interactive --topic home/temperature --priority 50
```
*Type a message like `temp=100C` and hit Enter. The subscriber terminal and the Web Dashboard will update instantly.*

**6. Single-Message Emergency Publish**
```bash
python3 -m client.client publish --topic home/fire --priority 255 --message "MANUAL FIRE OVERRIDE!"
```
*This sends one packet and disconnects instantly. Great for proving low latency.*

---

## Phase 4: The Climax — QoS Min-Heap Stress Test

This is the most important part of the Viva. You will prove that your Min-Heap prioritizes Fire Alarms over Battery updates when the network is congested.

**7. Trigger the Load Simulator**
```bash
python3 simulate_load.py
```
*This will blast 20 messages simultaneously over a single TCP socket 10 times in a row. Watch the Web Dashboard Priority Queue carefully—you will see the red Fire Alarms instantly jump ahead of the green Battery updates!*

---

## Phase 5: Event Sourcing & Time-Travel

To prove that your broker safely appends all binary frames to disk in `O(1)` time, demonstrate the Time-Travel feature.

**8. Replay History via CLI**
```bash
python3 -m client.client time-travel --topic home/fire --offset 0
```
*This will read the `broker_log.bin` file from byte 0 and instantly replay every single fire alarm that happened since the server was turned on.*
