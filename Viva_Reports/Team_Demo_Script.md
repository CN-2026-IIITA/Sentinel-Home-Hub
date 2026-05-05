# 🚀 Sentinel Home Hub: Team Demo Command Script

This guide contains every terminal command your team will need during the live Viva presentation, including the multi-computer Wi-Fi setup. **Keep this open on your screens during the demo.**

---

## Phase 1: Booting the Core Infrastructure (Host Laptop)

The **Host Laptop** will run the central broker. Find the Host's Wi-Fi IP address first (e.g., `192.168.1.50`).

**1. Start the TCP Broker (Listening on all network interfaces)**
```bash
python3 main.py --host 0.0.0.0
```
*Leave this running in the background.*

**2. Start the Web Dashboard Proxy**
```bash
python3 dashboard_server.py
```
*Once running, anyone on the Wi-Fi can open `http://<HOST_IP>:8080` in their web browser.*

---

## Phase 2: Simulating "Normal" Smart Home Activity (Host Laptop)

Before explaining the architecture, start the normal simulator so the Web Dashboard looks alive and active.

**3. Run the Normal Day Simulator**
```bash
python3 smart_home_simulator.py
```
*This will slowly tick temperature changes, open/close doors, drain the battery, and ping the fire alarm status.*

---

## Phase 3: CLI Demonstration (Client Laptops)

Have your team members open terminals on their **Client Laptops** to show the custom CLI routing data over the Wi-Fi. Replace `192.168.1.50` with your actual Host IP.

**4. The Universal Subscriber (Listening to multiple topics at once)**
```bash
python3 -m client.client subscribe --topic "home/fire,home/door,home/battery" --host 192.168.1.50
```

**5. The Interactive Publisher (Sending custom data)**
```bash
python3 -m client.client publish-interactive --topic home/temperature --priority 50 --host 192.168.1.50
```
*Type a message like `temp=100C` and hit Enter. It will instantly appear on the Web Dashboard!*

**6. Single-Message Emergency Publish**
```bash
python3 -m client.client publish --topic home/fire --priority 255 --message "MANUAL FIRE OVERRIDE!" --host 192.168.1.50
```

---

## Phase 4: The Climax — QoS Min-Heap Stress Test

This is the most important part of the Viva. You will prove that your Min-Heap prioritizes Fire Alarms over Battery updates when the network is congested.

**7. Trigger the Load Simulator (Can be run on Host or Client)**
```bash
python3 simulate_load.py
```
*(If running on a Client laptop, you would need to edit the script to point to the Host IP, so it's easiest to just run this script directly on the Host laptop).*

*Watch the Web Dashboard Priority Queue carefully—you will see the red Fire Alarms instantly jump ahead of the green Battery updates!*

---

## Phase 5: Event Sourcing & Time-Travel

To prove that your broker safely appends all binary frames to disk in `O(1)` time, demonstrate the Time-Travel feature.

**8. Replay History via CLI (Client Laptop)**
```bash
python3 -m client.client time-travel --topic home/fire --offset 0 --host 192.168.1.50
```
*This will instantly replay every single fire alarm that happened since the server was turned on.*
