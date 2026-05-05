import asyncio
import subprocess
import time

async def run_test():
    # start broker
    broker = await asyncio.create_subprocess_exec("python3", "main.py", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    await asyncio.sleep(2)
    
    # start subscriber
    sub = await asyncio.create_subprocess_exec("python3", "-m", "client.client", "subscribe", "--topic", "home/battery", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    await asyncio.sleep(1)
    
    # run load
    load = await asyncio.create_subprocess_exec("python3", "simulate_load.py", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    await asyncio.sleep(8) # wait for one burst
    
    load.terminate()
    sub.terminate()
    broker.terminate()
    
    sub_out, _ = await sub.communicate()
    print("Sub output:")
    print(sub_out.decode())

asyncio.run(run_test())
