#!/usr/bin/env python3
"""
VHF Comprehensive Message Stress Test — 500+ per message type
Uses Device B (/dev/cu.usbmodem1101) to send to Device A (!335e1be8) via LoRa.
Device A's USB is wedged but LoRa is working fine.

Message types:
  1. DM (text)     : 500 messages B→A
  2. Broadcast     : 500 messages from B
  3. Channel       : 500 messages (ch0 maluca)
  4. Long messages : 100 messages (200+ chars)
  5. Rapid burst   : 100 messages (2s interval)
  Total: ~1700 messages

Results logged to tests/comprehensive_results.json
"""

import subprocess
import time
import sys
import json
import os
import random
import string
from datetime import datetime

PORT_B = "/dev/cu.usbmodem1101"  # VHF Beam B (!335e1bdc)
DEST_A = "!335e1be8"             # VHF Beam A node ID
INTER_MSG_DELAY = 4              # seconds between messages (CLI needs ~3-5s per call)
RESULTS_FILE = os.path.join(os.path.dirname(__file__), "comprehensive_results.json")

results = {
    "start_time": datetime.now().isoformat(),
    "device_b_port": PORT_B,
    "device_a_node": DEST_A,
    "phases": {}
}

def send_msg(port, text, dest=None, channel=None, timeout=15):
    """Send a message via meshtastic CLI. Returns True on success."""
    cmd = ["meshtastic", "--port", port, "--sendtext", text]
    if dest:
        cmd += ["--dest", dest]
    if channel is not None:
        cmd += ["--ch-index", str(channel)]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.returncode == 0
    except subprocess.TimeoutExpired:
        return False
    except Exception:
        return False

def run_phase(name, count, send_fn, delay=INTER_MSG_DELAY):
    """Run a test phase, tracking success/failure."""
    print(f"\n{'='*60}")
    print(f"  Phase: {name} ({count} messages)")
    print(f"{'='*60}")

    success = 0
    fail = 0
    errors = []

    for i in range(1, count + 1):
        ok = send_fn(i)
        if ok:
            success += 1
        else:
            fail += 1
            errors.append(i)

        # Progress every 25 messages
        if i % 25 == 0 or i == count:
            pct = success / i * 100
            print(f"  [{i}/{count}] success={success} fail={fail} ({pct:.1f}%)")

        if i < count:
            time.sleep(delay)

    phase_result = {
        "total": count,
        "success": success,
        "fail": fail,
        "success_rate": f"{success/count*100:.1f}%",
        "failed_indices": errors[:50],  # cap at 50 to keep file manageable
    }
    results["phases"][name] = phase_result

    # Save after each phase
    with open(RESULTS_FILE, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"  Result: {success}/{count} ({success/count*100:.1f}%)")
    return phase_result

def random_text(length=40):
    return ''.join(random.choices(string.ascii_letters + string.digits + ' ', k=length))

def main():
    print("VHF Comprehensive Stress Test")
    print(f"Start: {datetime.now().isoformat()}")
    print(f"Device B: {PORT_B}")
    print(f"Target A: {DEST_A}")

    # Verify Device B is reachable
    print("\nVerifying Device B connectivity...")
    r = subprocess.run(["meshtastic", "--port", PORT_B, "--info"],
                      capture_output=True, text=True, timeout=30)
    if r.returncode != 0:
        print(f"ERROR: Cannot connect to Device B on {PORT_B}")
        sys.exit(1)
    print("Device B OK")

    # ========== PHASE 1: DM B→A (500 messages) ==========
    def send_dm(i):
        text = f"DM-{i:04d} {random_text(30)}"
        return send_msg(PORT_B, text, dest=DEST_A)

    run_phase("DM_B_to_A", 500, send_dm)

    # ========== PHASE 2: Broadcast (500 messages) ==========
    def send_broadcast(i):
        text = f"BC-{i:04d} {random_text(30)}"
        return send_msg(PORT_B, text)  # no dest = broadcast

    run_phase("Broadcast", 500, send_broadcast)

    # ========== PHASE 3: Channel messages (500 on ch0) ==========
    def send_channel(i):
        text = f"CH-{i:04d} {random_text(30)}"
        return send_msg(PORT_B, text, channel=0)

    run_phase("Channel_ch0", 500, send_channel)

    # ========== PHASE 4: Long messages (100 near-MTU) ==========
    def send_long(i):
        text = f"LONG-{i:04d} " + random_text(210)
        return send_msg(PORT_B, text, dest=DEST_A, timeout=20)

    run_phase("Long_messages", 100, send_long, delay=6)

    # ========== PHASE 5: Rapid burst (100 at 2s interval) ==========
    def send_rapid(i):
        text = f"RAPID-{i:04d} {random_text(20)}"
        return send_msg(PORT_B, text, dest=DEST_A, timeout=10)

    run_phase("Rapid_burst", 100, send_rapid, delay=2)

    # ========== Summary ==========
    results["end_time"] = datetime.now().isoformat()

    total_sent = 0
    total_success = 0
    print(f"\n{'='*60}")
    print("  FINAL RESULTS")
    print(f"{'='*60}")
    for name, data in results["phases"].items():
        total_sent += data["total"]
        total_success += data["success"]
        print(f"  {name:20s}: {data['success']:4d}/{data['total']:4d} ({data['success_rate']})")

    overall = total_success / total_sent * 100 if total_sent > 0 else 0
    print(f"  {'TOTAL':20s}: {total_success:4d}/{total_sent:4d} ({overall:.1f}%)")
    results["total_sent"] = total_sent
    results["total_success"] = total_success
    results["overall_rate"] = f"{overall:.1f}%"

    with open(RESULTS_FILE, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"\nResults saved to: {RESULTS_FILE}")
    print(f"End: {datetime.now().isoformat()}")

if __name__ == "__main__":
    main()
