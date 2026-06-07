#!/usr/bin/env python3
"""
VHF T-Beam Supreme S3 — Comprehensive MeshReliable Test Suite
Tests messaging between two T-Beam devices over 144 MHz VHF.
"""

import subprocess
import time
import sys
import threading
import queue
import serial
import json
from datetime import datetime

PORT_A = "/dev/cu.usbmodem101"   # T-Beam #1 — VHF Beam A (!335e1be8)
PORT_B = "/dev/cu.usbmodem1101"  # T-Beam #2 — VHF Beam B (!335e1bdc)
NODE_A = "!335e1be8"
NODE_B = "!335e1bdc"

RESULTS = []

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")

def run_meshtastic(port, args, timeout=30):
    """Run a meshtastic CLI command and return (success, output)."""
    cmd = ["meshtastic", "--port", port] + args
    log(f"  CMD: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        output = result.stdout + result.stderr
        return result.returncode == 0, output.strip()
    except subprocess.TimeoutExpired:
        return False, "TIMEOUT"
    except Exception as e:
        return False, str(e)

def wait_between_tests(seconds=5):
    """Wait between tests to let radio settle."""
    log(f"  Waiting {seconds}s for radio to settle...")
    time.sleep(seconds)

class SerialMonitor:
    """Monitor serial output from a device in a background thread."""
    def __init__(self, port, name):
        self.port = port
        self.name = name
        self.lines = []
        self.running = False
        self.thread = None

    def start(self):
        self.running = True
        self.lines = []
        self.thread = threading.Thread(target=self._reader, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=3)

    def _reader(self):
        try:
            s = serial.Serial(self.port, 115200, timeout=1)
            while self.running:
                line = s.readline().decode('utf-8', errors='replace').strip()
                if line:
                    self.lines.append(line)
            s.close()
        except Exception as e:
            self.lines.append(f"SERIAL_ERROR: {e}")

    def get_lines(self):
        return list(self.lines)

    def clear(self):
        self.lines = []

    def find(self, pattern):
        """Return lines containing pattern."""
        return [l for l in self.lines if pattern in l]

def record_result(test_name, passed, details=""):
    status = "PASS" if passed else "FAIL"
    RESULTS.append({"test": test_name, "passed": passed, "details": details})
    log(f"  [{status}] {test_name}" + (f" — {details}" if details else ""))

def kill_port_holders():
    """Kill any processes holding the serial ports."""
    for port in [PORT_A, PORT_B]:
        subprocess.run(f"lsof {port} 2>/dev/null | grep -v COMMAND | awk '{{print $2}}' | xargs kill -9 2>/dev/null",
                       shell=True, capture_output=True)
    time.sleep(2)

# ============================================================
# TEST 1: Direct Message A -> B
# ============================================================
def test_dm_a_to_b():
    log("TEST 1: Direct Message A -> B")
    kill_port_holders()
    msg = f"Hello from A {int(time.time()) % 10000}"
    ok, out = run_meshtastic(PORT_A, ["--dest", NODE_B, "--sendtext", msg])
    record_result("DM A->B send", ok, out[:200])
    return ok

# ============================================================
# TEST 2: Direct Message B -> A
# ============================================================
def test_dm_b_to_a():
    log("TEST 2: Direct Message B -> A")
    kill_port_holders()
    msg = f"Hello from B {int(time.time()) % 10000}"
    ok, out = run_meshtastic(PORT_B, ["--dest", NODE_A, "--sendtext", msg])
    record_result("DM B->A send", ok, out[:200])
    return ok

# ============================================================
# TEST 3: Broadcast message from A
# ============================================================
def test_broadcast_a():
    log("TEST 3: Broadcast from A")
    kill_port_holders()
    msg = f"Broadcast from A {int(time.time()) % 10000}"
    ok, out = run_meshtastic(PORT_A, ["--sendtext", msg])
    record_result("Broadcast from A", ok, out[:200])
    return ok

# ============================================================
# TEST 4: Broadcast message from B
# ============================================================
def test_broadcast_b():
    log("TEST 4: Broadcast from B")
    kill_port_holders()
    msg = f"Broadcast from B {int(time.time()) % 10000}"
    ok, out = run_meshtastic(PORT_B, ["--sendtext", msg])
    record_result("Broadcast from B", ok, out[:200])
    return ok

# ============================================================
# TEST 5: Node discovery — both see each other
# ============================================================
def test_node_discovery():
    log("TEST 5: Node Discovery")
    kill_port_holders()
    ok_a, out_a = run_meshtastic(PORT_A, ["--nodes"])
    has_b = NODE_B in out_a if ok_a else False
    record_result("A sees B in mesh", has_b, f"Found {NODE_B}" if has_b else out_a[:200])

    time.sleep(3)
    kill_port_holders()
    ok_b, out_b = run_meshtastic(PORT_B, ["--nodes"])
    has_a = NODE_A in out_b if ok_b else False
    record_result("B sees A in mesh", has_a, f"Found {NODE_A}" if has_a else out_b[:200])
    return has_b and has_a

# ============================================================
# TEST 6: Position request
# ============================================================
def test_position_request():
    log("TEST 6: Position Request A -> B")
    kill_port_holders()
    ok, out = run_meshtastic(PORT_A, ["--dest", NODE_B, "--request-position"])
    record_result("Position request A->B", ok, out[:200])
    return ok

# ============================================================
# TEST 7: Telemetry check
# ============================================================
def test_telemetry():
    log("TEST 7: Telemetry (device metrics)")
    kill_port_holders()
    ok, out = run_meshtastic(PORT_A, ["--info"])
    has_voltage = "voltage" in out.lower() if ok else False
    has_battery = "batteryLevel" in out if ok else False
    record_result("Telemetry has voltage", has_voltage)
    record_result("Telemetry has battery", has_battery)
    return has_voltage and has_battery

# ============================================================
# TEST 8: Channel verification
# ============================================================
def test_channels():
    log("TEST 8: Channel Verification")
    kill_port_holders()
    ok_a, out_a = run_meshtastic(PORT_A, ["--ch-index", "0", "--ch-get", "name"])
    has_maluca = "maluca" in out_a.lower() if ok_a else False
    record_result("Channel 0 = maluca on A", has_maluca, out_a[:200])

    time.sleep(3)
    kill_port_holders()
    ok_b, out_b = run_meshtastic(PORT_B, ["--ch-index", "0", "--ch-get", "name"])
    has_maluca_b = "maluca" in out_b.lower() if ok_b else False
    record_result("Channel 0 = maluca on B", has_maluca_b, out_b[:200])
    return has_maluca and has_maluca_b

# ============================================================
# TEST 9: Radio config verification (VHF)
# ============================================================
def test_radio_config():
    log("TEST 9: Radio Config Verification (VHF)")
    kill_port_holders()
    ok, out = run_meshtastic(PORT_A, ["--get", "lora"])
    region_ok = "region: 28" in out if ok else False
    tx_ok = "tx_enabled: True" in out if ok else False
    record_result("Region = ITU2_2M (28)", region_ok, out[:300])
    record_result("TX enabled", tx_ok)
    return region_ok and tx_ok

# ============================================================
# TEST 10: Rapid message burst (3 messages)
# ============================================================
def test_burst_messages():
    log("TEST 10: Rapid Burst (3 DMs A->B)")
    kill_port_holders()
    successes = 0
    for i in range(3):
        msg = f"Burst {i+1} ts={int(time.time()) % 10000}"
        ok, out = run_meshtastic(PORT_A, ["--dest", NODE_B, "--sendtext", msg])
        if ok:
            successes += 1
        time.sleep(5)
        kill_port_holders()
    record_result(f"Burst DMs sent ({successes}/3)", successes >= 2, f"{successes}/3 succeeded")
    return successes >= 2

# ============================================================
# TEST 11: Long message
# ============================================================
def test_long_message():
    log("TEST 11: Long Message (200 chars)")
    kill_port_holders()
    msg = "A" * 200 + f" {int(time.time()) % 10000}"
    ok, out = run_meshtastic(PORT_A, ["--dest", NODE_B, "--sendtext", msg])
    record_result("Long message (200 chars)", ok, out[:200])
    return ok

# ============================================================
# TEST 12: Monitor serial for TX/RX confirmation
# ============================================================
def test_serial_tx_rx():
    log("TEST 12: Serial Monitor — TX/RX over VHF")
    kill_port_holders()
    time.sleep(2)

    # Start monitoring B
    try:
        ser_b = serial.Serial(PORT_B, 115200, timeout=1)
    except Exception as e:
        record_result("Serial monitor open", False, str(e))
        return False

    collected = []
    # Send from A
    msg = f"SerialTest {int(time.time()) % 10000}"
    ok, out = run_meshtastic(PORT_A, ["--dest", NODE_B, "--sendtext", msg])
    if not ok:
        record_result("Serial test send", False, out[:200])
        ser_b.close()
        return False

    # Collect B's serial for 15 seconds
    end = time.time() + 15
    while time.time() < end:
        line = ser_b.readline().decode('utf-8', errors='replace').strip()
        if line:
            collected.append(line)
    ser_b.close()

    rx_lines = [l for l in collected if "RX" in l or "Received" in l or "TEXT_MESSAGE" in l]
    has_rx = len(rx_lines) > 0
    record_result("B received packet (serial)", has_rx,
                  f"Found {len(rx_lines)} RX lines" if has_rx else "No RX lines found in serial")

    # Check for the backup polling mechanism
    missed_lines = [l for l in collected if "caught missed" in l]
    if missed_lines:
        log(f"  (Expected: {len(missed_lines)} 'caught missed TX/RX_DONE' events — backup polling working)")

    return has_rx

# ============================================================
# MAIN
# ============================================================
def main():
    log("=" * 60)
    log("VHF T-Beam Supreme MeshReliable Test Suite")
    log(f"Device A: {NODE_A} on {PORT_A}")
    log(f"Device B: {NODE_B} on {PORT_B}")
    log("=" * 60)

    tests = [
        ("Radio Config", test_radio_config),
        ("Channels", test_channels),
        ("Node Discovery", test_node_discovery),
        ("DM A->B", test_dm_a_to_b),
        ("DM B->A", test_dm_b_to_a),
        ("Broadcast A", test_broadcast_a),
        ("Broadcast B", test_broadcast_b),
        ("Position Request", test_position_request),
        ("Telemetry", test_telemetry),
        ("Long Message", test_long_message),
        ("Burst Messages", test_burst_messages),
        ("Serial TX/RX", test_serial_tx_rx),
    ]

    for name, test_fn in tests:
        log(f"\n{'='*40}")
        try:
            test_fn()
        except Exception as e:
            record_result(f"{name} (EXCEPTION)", False, str(e))
        wait_between_tests(5)

    # Summary
    log(f"\n{'='*60}")
    log("TEST SUMMARY")
    log(f"{'='*60}")
    passed = sum(1 for r in RESULTS if r["passed"])
    failed = sum(1 for r in RESULTS if not r["passed"])
    for r in RESULTS:
        status = "PASS" if r["passed"] else "FAIL"
        log(f"  [{status}] {r['test']}")
    log(f"\nTotal: {passed} passed, {failed} failed out of {len(RESULTS)} tests")

    # Save results
    with open("tests/vhf_test_results.json", "w") as f:
        json.dump({"timestamp": datetime.now().isoformat(), "results": RESULTS,
                    "summary": {"passed": passed, "failed": failed, "total": len(RESULTS)}}, f, indent=2)
    log(f"Results saved to tests/vhf_test_results.json")

if __name__ == "__main__":
    main()
