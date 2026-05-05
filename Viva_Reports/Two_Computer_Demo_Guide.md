# 🚀 Sentinel Home Hub: Two-Computer Viva Setup

This guide is perfectly tailored for your specific demo strategy: **Your laptop** will handle the heavy lifting (the Broker, the CLI demo, and the Dashboard backend), while the **other laptop** will simply act as a beautiful visual display monitor.

---

## 💻 Laptop 1: Your Laptop (The Core Server & CLI)

First, find your Wi-Fi IP address (e.g., `192.168.1.50`). 

**1. Start the Core Broker (Listening on Wi-Fi)**
```bash
python3 main.py --host 0.0.0.0
```
*Leave this running in the background.*

**2. Start the Web Dashboard Proxy**
```bash
python3 dashboard_server.py
```
*Leave this running in the background.*

**3. Run the CLI Demonstrations (Localhost is fine)**
Since you are running the CLI on the *same laptop* as the broker, you don't even need to type the IP address here!

*   **Subscriber:**
    ```bash
    python3 -m client.client subscribe --topic "home/fire,home/door,home/battery"
    ```
*   **Interactive Publisher:**
    ```bash
    python3 -m client.client publish-interactive --topic home/temperature --priority 50
    ```
*   **QoS Burst Load Test:**
    ```bash
    python3 simulate_load.py
    ```

---

## 🖥️ Laptop 2: The Other Laptop (The Visual Dashboard)

The beauty of this setup is that the second laptop **does not need to run Python or use the terminal at all!**

**1. Open the Web Browser**
Make sure Laptop 2 is connected to the exact same Wi-Fi network as your laptop.

**2. Type your IP Address into the URL bar**
If your IP address was `192.168.1.50`, have them type:
```text
http://192.168.1.50:8080
```

### The Result:
As you type commands into the CLI on your laptop (Laptop 1), the resulting messages will fly over the Wi-Fi and instantly light up the Dashboard animations on the second laptop (Laptop 2) for the professor to watch!
