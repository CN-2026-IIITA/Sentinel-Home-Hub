import struct
from broker.protocol import decode_header
count = 0
fire = 0
battery = 0
with open("broker_log.bin", "rb") as f:
    while True:
        hdr = f.read(10)
        if len(hdr) < 10: break
        cmd, tid, pri, plen = decode_header(hdr)
        f.read(plen)
        count += 1
        if pri == 255: fire += 1
        if pri == 0: battery += 1
print(f"Total: {count}, Fire: {fire}, Battery: {battery}")
