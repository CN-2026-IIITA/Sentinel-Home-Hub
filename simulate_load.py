import asyncio
from client.client import mode_publish

async def simulate():
    print("Starting continuous load simulation. Press Ctrl+C to stop.")
    while True:
        tasks = []
        # Send Fire Alarms (high priority)
        for _ in range(50):
            tasks.append(mode_publish("127.0.0.1", 9999, "sensors/fire", 255, "Fire detected in sector 7G"))
        
        # Send Battery Status (low priority)
        for _ in range(200):
            tasks.append(mode_publish("127.0.0.1", 9999, "sensors/battery", 0, "Battery at 80%"))
            
        await asyncio.gather(*tasks)
        print("Burst sent! Waiting 5 seconds...")
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(simulate())
