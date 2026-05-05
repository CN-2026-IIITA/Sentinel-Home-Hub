import asyncio
from broker.protocol import CMD_PUBLISH, encode, topic_hash

async def simulate():
    print("Starting continuous load simulation. Press Ctrl+C to stop.")
    
    tid_fire = topic_hash("home/fire")
    tid_battery = topic_hash("home/battery")
    
    while True:
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", 9999)
            
            # Pack all 250 messages into a single TCP stream to test Packet Sticking!
            burst_data = bytearray()
            
            # Send 15 Fire Alarms (high priority)
            for _ in range(15):
                burst_data.extend(encode(CMD_PUBLISH, tid_fire, 255, b"Fire detected in sector 7G"))
            
            # Send 5 Battery Status (low priority)
            for _ in range(5):
                burst_data.extend(encode(CMD_PUBLISH, tid_battery, 0, b"Battery at 80%"))
                
            writer.write(burst_data)
            await writer.drain()
            
            print("Burst of 20 messages sent over 1 socket! Waiting 5 seconds...")
            writer.close()
            await writer.wait_closed()
            
            await asyncio.sleep(5)
            
        except ConnectionRefusedError:
            print("Broker offline. Retrying in 5 seconds...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(simulate())
