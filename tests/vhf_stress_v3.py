#!/usr/bin/env python3
"""VHF stress test v3 — file-based logging, 500+ per type."""
import meshtastic, meshtastic.serial_interface
import time, json, os, random, string
from datetime import datetime

PORT = "/dev/cu.usbmodem1101"
DEST = 0x335e1be8
LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stress_v3.log")
RES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stress_v3_results.json")

results = {"start": datetime.now().isoformat(), "phases": {}}

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    with open(LOG, 'a') as f:
        f.write(line + '\n')
    print(line, flush=True)

def rand(n=35):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=n))

def phase(name, count, fn, delay=1.0):
    log(f"=== {name}: {count} msgs, delay={delay}s ===")
    ok = 0
    fail = 0
    errs = []
    t0 = time.time()
    for i in range(1, count+1):
        try:
            fn(i)
            ok += 1
        except Exception as e:
            fail += 1
            errs.append(i)
            if i <= 5:
                log(f"  ERR@{i}: {e}")
        if i % 50 == 0 or i == count:
            log(f"  [{i}/{count}] ok={ok} fail={fail} ({ok/i*100:.1f}%)")
        if i < count:
            time.sleep(delay)
    elapsed = time.time() - t0
    r = {"total": count, "ok": ok, "fail": fail, "rate": f"{ok/count*100:.1f}%",
         "secs": round(elapsed,1), "errors": errs[:20]}
    results["phases"][name] = r
    with open(RES, 'w') as f:
        json.dump(results, f, indent=2)
    log(f"  DONE: {ok}/{count} ({ok/count*100:.1f}%) in {elapsed:.0f}s")
    return r

# Clear old log
open(LOG, 'w').close()
log("Connecting...")
iface = meshtastic.serial_interface.SerialInterface(PORT)
log(f"Connected to {iface.getMyNodeInfo().get('user',{}).get('longName','?')}")
time.sleep(2)

# Phase 1: DMs
phase("DM_B_to_A", 500,
      lambda i: iface.sendText(f"DM-{i:04d} {rand()}", destinationId=DEST, wantAck=False),
      delay=1.0)
time.sleep(10)

# Phase 2: Broadcasts
phase("Broadcast", 500,
      lambda i: iface.sendText(f"BC-{i:04d} {rand()}", wantAck=False),
      delay=1.0)
time.sleep(10)

# Phase 3: Channel
phase("Channel_ch0", 500,
      lambda i: iface.sendText(f"CH-{i:04d} {rand()}", channelIndex=0, wantAck=False),
      delay=1.0)
time.sleep(10)

# Phase 4: Long msgs
phase("Long_msgs", 100,
      lambda i: iface.sendText(f"LG-{i:04d} " + rand(200), destinationId=DEST, wantAck=False),
      delay=2.0)
time.sleep(10)

# Phase 5: Rapid
phase("Rapid", 100,
      lambda i: iface.sendText(f"RP-{i:04d} {rand(20)}", destinationId=DEST, wantAck=False),
      delay=0.5)

results["end"] = datetime.now().isoformat()
total = sum(p["ok"] for p in results["phases"].values())
total_n = sum(p["total"] for p in results["phases"].values())
results["total_ok"] = total
results["total_n"] = total_n
results["overall"] = f"{total/total_n*100:.1f}%"
with open(RES, 'w') as f:
    json.dump(results, f, indent=2)
log(f"TOTAL: {total}/{total_n} ({total/total_n*100:.1f}%)")
iface.close()
