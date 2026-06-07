#!/usr/bin/env python3
"""
VHF Fast Stress Test — 500+ per message type using Python API
Uses persistent serial connection for much faster throughput (~1s/msg vs 5-6s/msg).

Message types tested:
  1. DM text       : 500 messages B→A
  2. Broadcast text: 500 messages from B
  3. Channel text  : 500 messages on ch0
  4. Long messages : 100 messages (200+ chars)
  5. Rapid burst   : 100 messages (0.5s interval)
  Total: ~1700 messages

Results logged to tests/fast_stress_results.json
"""

import time
import sys
import json
import os
import random
import string
import struct
from datetime import datetime

# Force unbuffered output
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', buffering=1)
sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', buffering=1)

# Meshtastic Python API
import meshtastic
import meshtastic.serial_interface
from pubsub import pub

PORT_B = "/dev/cu.usbmodem1101"  # VHF Beam B (!335e1bdc)
DEST_A_NUM = 0x335e1be8          # VHF Beam A node number
INTER_MSG_DELAY = 1.0            # seconds between messages (LoRa TX queue)
RESULTS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fast_stress_results.json")

# Track ACKs received
ack_tracker = {}
nack_count = 0
ack_count = 0

def on_receive(packet, interface):
    """Track received ACKs/NACKs."""
    global ack_count, nack_count
    if packet.get("decoded", {}).get("portnum") == "ROUTING_APP":
        req_id = packet.get("decoded", {}).get("requestId", 0)
        routing = packet.get("decoded", {}).get("routing", {})
        if routing.get("errorReason") == "NONE" or "SUCCESS" in str(routing):
            ack_count += 1
            if req_id in ack_tracker:
                ack_tracker[req_id] = "ACK"
        else:
            nack_count += 1
            if req_id in ack_tracker:
                ack_tracker[req_id] = "NACK"

results = {
    "start_time": datetime.now().isoformat(),
    "device_b_port": PORT_B,
    "device_a_node": hex(DEST_A_NUM),
    "phases": {}
}

def random_text(length=35):
    return ''.join(random.choices(string.ascii_letters + string.digits + ' ', k=length))

def save_results():
    with open(RESULTS_FILE, 'w') as f:
        json.dump(results, f, indent=2)

def run_phase(name, count, send_fn, delay=INTER_MSG_DELAY):
    """Run a test phase, tracking send success/failure."""
    print(f"\n{'='*60}")
    print(f"  Phase: {name} ({count} messages, {delay}s delay)")
    print(f"{'='*60}")
    sys.stdout.flush()

    success = 0
    fail = 0
    errors = []
    start_time = time.time()

    for i in range(1, count + 1):
        try:
            ok = send_fn(i)
            if ok:
                success += 1
            else:
                fail += 1
                errors.append(i)
        except Exception as e:
            fail += 1
            errors.append(i)
            if i <= 3 or i % 100 == 0:
                print(f"  ERROR at msg {i}: {e}")

        # Progress every 50 messages
        if i % 50 == 0 or i == count:
            elapsed = time.time() - start_time
            rate = i / elapsed if elapsed > 0 else 0
            pct = success / i * 100
            print(f"  [{i}/{count}] ok={success} fail={fail} ({pct:.1f}%) "
                  f"rate={rate:.1f} msg/s elapsed={elapsed:.0f}s")
            sys.stdout.flush()

        if i < count:
            time.sleep(delay)

    elapsed = time.time() - start_time
    phase_result = {
        "total": count,
        "success": success,
        "fail": fail,
        "success_rate": f"{success/count*100:.1f}%",
        "elapsed_seconds": round(elapsed, 1),
        "msg_per_second": round(count / elapsed, 2) if elapsed > 0 else 0,
        "failed_indices": errors[:50],
    }
    results["phases"][name] = phase_result
    save_results()

    print(f"  Result: {success}/{count} ({success/count*100:.1f}%) in {elapsed:.0f}s")
    sys.stdout.flush()
    return phase_result

def main():
    global ack_count, nack_count

    print("VHF Fast Stress Test (Python API)")
    print(f"Start: {datetime.now().isoformat()}")
    print(f"Device B: {PORT_B}")
    print(f"Target A: {hex(DEST_A_NUM)}")
    sys.stdout.flush()

    # Connect to Device B
    print("\nConnecting to Device B...")
    sys.stdout.flush()
    try:
        iface = meshtastic.serial_interface.SerialInterface(PORT_B)
    except Exception as e:
        print(f"ERROR: Cannot connect to Device B: {e}")
        sys.exit(1)

    # Subscribe to receive events for ACK tracking
    pub.subscribe(on_receive, "meshtastic.receive")

    my_info = iface.getMyNodeInfo()
    print(f"Connected: {my_info.get('user', {}).get('longName', 'unknown')}")
    print(f"Node num: {hex(my_info.get('num', 0))}")
    sys.stdout.flush()

    time.sleep(2)  # Let connection stabilize

    # ========== PHASE 1: DM B→A (500 messages) ==========
    def send_dm(i):
        text = f"DM-{i:04d} {random_text()}"
        try:
            iface.sendText(text, destinationId=DEST_A_NUM, wantAck=False)
            return True
        except Exception as e:
            if i <= 3:
                print(f"  DM send error: {e}")
                sys.stdout.flush()
            return False

    run_phase("DM_B_to_A", 500, send_dm, delay=1.0)

    # Brief pause between phases
    print("\n  Pausing 10s between phases...")
    sys.stdout.flush()
    time.sleep(10)

    # ========== PHASE 2: Broadcast (500 messages) ==========
    def send_broadcast(i):
        text = f"BC-{i:04d} {random_text()}"
        try:
            iface.sendText(text, wantAck=False)
            return True
        except Exception:
            return False

    run_phase("Broadcast", 500, send_broadcast, delay=1.0)

    print("\n  Pausing 10s between phases...")
    sys.stdout.flush()
    time.sleep(10)

    # ========== PHASE 3: Channel messages (500 on ch0) ==========
    def send_channel(i):
        text = f"CH-{i:04d} {random_text()}"
        try:
            iface.sendText(text, channelIndex=0, wantAck=False)
            return True
        except Exception:
            return False

    run_phase("Channel_ch0", 500, send_channel, delay=1.0)

    print("\n  Pausing 10s between phases...")
    sys.stdout.flush()
    time.sleep(10)

    # ========== PHASE 4: Long messages (100 near-MTU) ==========
    def send_long(i):
        text = f"LONG-{i:04d} " + random_text(200)
        try:
            iface.sendText(text, destinationId=DEST_A_NUM, wantAck=False)
            return True
        except Exception:
            return False

    run_phase("Long_messages", 100, send_long, delay=2.0)

    print("\n  Pausing 10s between phases...")
    sys.stdout.flush()
    time.sleep(10)

    # ========== PHASE 5: Rapid burst (100 at 0.5s interval) ==========
    def send_rapid(i):
        text = f"RAPID-{i:04d} {random_text(20)}"
        try:
            iface.sendText(text, destinationId=DEST_A_NUM, wantAck=False)
            return True
        except Exception:
            return False

    run_phase("Rapid_burst", 100, send_rapid, delay=0.5)

    # ========== Summary ==========
    results["end_time"] = datetime.now().isoformat()
    results["ack_count"] = ack_count
    results["nack_count"] = nack_count

    total_sent = 0
    total_success = 0
    print(f"\n{'='*60}")
    print("  FINAL RESULTS")
    print(f"{'='*60}")
    for name, data in results["phases"].items():
        total_sent += data["total"]
        total_success += data["success"]
        print(f"  {name:20s}: {data['success']:4d}/{data['total']:4d} ({data['success_rate']}) "
              f"@ {data['msg_per_second']} msg/s")

    overall = total_success / total_sent * 100 if total_sent > 0 else 0
    print(f"  {'TOTAL':20s}: {total_success:4d}/{total_sent:4d} ({overall:.1f}%)")
    print(f"  ACKs received: {ack_count}, NACKs: {nack_count}")
    results["total_sent"] = total_sent
    results["total_success"] = total_success
    results["overall_rate"] = f"{overall:.1f}%"

    save_results()

    print(f"\nResults saved to: {RESULTS_FILE}")
    print(f"End: {datetime.now().isoformat()}")
    sys.stdout.flush()

    # Cleanup
    try:
        iface.close()
    except:
        pass

if __name__ == "__main__":
    main()
