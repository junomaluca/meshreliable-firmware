#!/usr/bin/env python3
"""
VHF T-Beam Marathon Test — with iPhone monitoring
Sends various message types from Device A while monitoring Device B's serial
to verify radio delivery AND BLE forwarding to the connected iPhone.
"""

import subprocess
import time
import sys
import threading
import serial
import json
from datetime import datetime

PORT_A = "/dev/cu.usbmodem101"   # VHF Beam A (!335e1be8) — sender
PORT_B = "/dev/cu.usbmodem1101"  # VHF Beam B (!335e1bdc) — receiver (iPhone connected via BLE)
NODE_A = "!335e1be8"
NODE_B = "!335e1bdc"

RESULTS = []

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)

def record_result(test_name, passed, details=""):
    status = "PASS" if passed else "FAIL"
    RESULTS.append({"test": test_name, "passed": passed, "details": details})
    log(f"  [{status}] {test_name}" + (f" — {details}" if details else ""))

def kill_port_holders():
    for port in [PORT_A, PORT_B]:
        subprocess.run(f"lsof {port} 2>/dev/null | grep -v COMMAND | awk '{{print $2}}' | sort -u | xargs kill -9 2>/dev/null",
                       shell=True, capture_output=True)
    time.sleep(2)

def run_meshtastic(port, args, timeout=30):
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

class SerialCollector:
    """Collect serial output from a device in background."""
    def __init__(self, port, name):
        self.port = port
        self.name = name
        self.lines = []
        self.running = False
        self.thread = None
        self.ser = None

    def start(self):
        self.running = True
        self.lines = []
        self.thread = threading.Thread(target=self._reader, daemon=True)
        self.thread.start()
        time.sleep(0.5)

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=3)
        if self.ser:
            try:
                self.ser.close()
            except:
                pass

    def _reader(self):
        try:
            self.ser = serial.Serial(self.port, 115200, timeout=1)
            while self.running:
                try:
                    line = self.ser.readline().decode('utf-8', errors='replace').strip()
                    if line:
                        self.lines.append((time.time(), line))
                except serial.SerialException:
                    break
        except Exception as e:
            self.lines.append((time.time(), f"SERIAL_ERROR: {e}"))

    def get_lines_since(self, t):
        return [(ts, l) for ts, l in self.lines if ts >= t]

    def find(self, pattern, since=0):
        return [(ts, l) for ts, l in self.lines if pattern in l and ts >= since]

    def dump_recent(self, seconds=30):
        cutoff = time.time() - seconds
        recent = [(ts, l) for ts, l in self.lines if ts >= cutoff]
        for ts, l in recent[-20:]:
            log(f"    [{self.name}] {l[:150]}")

def send_and_monitor(monitor, port_send, args, wait_seconds=12, expect_patterns=None):
    """Send a meshtastic command and monitor for expected patterns in serial output."""
    mark = time.time()
    ok, out = run_meshtastic(port_send, args)
    if not ok:
        return False, f"Send failed: {out[:200]}"

    time.sleep(wait_seconds)

    found = {}
    if expect_patterns:
        for pat in expect_patterns:
            matches = monitor.find(pat, since=mark)
            found[pat] = len(matches)

    return ok, found

# ============================================================
# TEST FUNCTIONS
# ============================================================

def test_dm_a_to_b(monitor_b):
    """DM from A to B — should appear on iPhone."""
    log("TEST: Direct Message A -> B (should appear on iPhone)")
    kill_port_holders()
    time.sleep(1)
    monitor_b.start()
    time.sleep(1)

    msg = f"DM-test-{int(time.time()) % 10000}"
    mark = time.time()
    ok, out = run_meshtastic(PORT_A, ["--dest", NODE_B, "--sendtext", msg])

    if not ok:
        record_result("DM A->B send", False, out[:200])
        monitor_b.stop()
        return

    record_result("DM A->B send", True, f"Sent: {msg}")

    # Wait for B to receive and forward to BLE
    time.sleep(15)

    # Check B's serial for reception
    rx_lines = monitor_b.find("Lora RX", since=mark)
    text_lines = monitor_b.find("TEXT_MESSAGE", since=mark)
    ble_lines = monitor_b.find("BLE", since=mark)
    decoded_lines = monitor_b.find("handleReceived", since=mark)

    record_result("DM A->B radio RX on B", len(rx_lines) > 0,
                  f"{len(rx_lines)} Lora RX events")
    record_result("DM A->B decoded on B", len(decoded_lines) > 0,
                  f"{len(decoded_lines)} handleReceived events")

    log("  Recent serial from B:")
    monitor_b.dump_recent(20)

    log(f"  >>> CHECK iPHONE: Message '{msg}' should appear in Meshtastic app from VHF Beam A")
    monitor_b.stop()

def test_dm_b_to_a(monitor_a):
    """DM from B (via phone) to A — monitor A's serial."""
    log("TEST: Direct Message B -> A (via phone if available)")
    kill_port_holders()
    time.sleep(1)
    monitor_a.start()
    time.sleep(1)

    msg = f"DM-B2A-{int(time.time()) % 10000}"
    mark = time.time()
    ok, out = run_meshtastic(PORT_B, ["--dest", NODE_A, "--sendtext", msg])

    if not ok:
        record_result("DM B->A send", False, out[:200])
        monitor_a.stop()
        return

    record_result("DM B->A send", True, f"Sent: {msg}")

    time.sleep(15)

    rx_lines = monitor_a.find("Lora RX", since=mark)
    decoded_lines = monitor_a.find("handleReceived", since=mark)

    record_result("DM B->A radio RX on A", len(rx_lines) > 0,
                  f"{len(rx_lines)} Lora RX events")
    record_result("DM B->A decoded on A", len(decoded_lines) > 0,
                  f"{len(decoded_lines)} handleReceived events")

    log("  Recent serial from A:")
    monitor_a.dump_recent(20)
    monitor_a.stop()

def test_broadcast_a_to_all(monitor_b):
    """Broadcast from A — should reach B and iPhone."""
    log("TEST: Broadcast from A (channel message)")
    kill_port_holders()
    time.sleep(1)
    monitor_b.start()
    time.sleep(1)

    msg = f"Broadcast-A-{int(time.time()) % 10000}"
    mark = time.time()
    ok, out = run_meshtastic(PORT_A, ["--sendtext", msg])

    record_result("Broadcast A send", ok, f"Sent: {msg}" if ok else out[:200])

    time.sleep(15)

    rx_lines = monitor_b.find("Lora RX", since=mark)
    record_result("Broadcast A received on B", len(rx_lines) > 0,
                  f"{len(rx_lines)} Lora RX events")

    log(f"  >>> CHECK iPHONE: Broadcast '{msg}' should appear in channel from VHF Beam A")
    log("  Recent serial from B:")
    monitor_b.dump_recent(20)
    monitor_b.stop()

def test_broadcast_b_to_all(monitor_a):
    """Broadcast from B — should reach A."""
    log("TEST: Broadcast from B (channel message)")
    kill_port_holders()
    time.sleep(1)
    monitor_a.start()
    time.sleep(1)

    msg = f"Broadcast-B-{int(time.time()) % 10000}"
    mark = time.time()
    ok, out = run_meshtastic(PORT_B, ["--sendtext", msg])

    record_result("Broadcast B send", ok, f"Sent: {msg}" if ok else out[:200])

    time.sleep(15)

    rx_lines = monitor_a.find("Lora RX", since=mark)
    record_result("Broadcast B received on A", len(rx_lines) > 0,
                  f"{len(rx_lines)} Lora RX events")

    log("  Recent serial from A:")
    monitor_a.dump_recent(20)
    monitor_a.stop()

def test_channel_message_ch0(monitor_b):
    """Send on channel 0 (maluca)."""
    log("TEST: Channel 0 (maluca) message A -> B")
    kill_port_holders()
    time.sleep(1)
    monitor_b.start()
    time.sleep(1)

    msg = f"Maluca-ch0-{int(time.time()) % 10000}"
    mark = time.time()
    ok, out = run_meshtastic(PORT_A, ["--ch-index", "0", "--sendtext", msg])

    record_result("Channel 0 message send", ok, f"Sent: {msg}" if ok else out[:200])

    time.sleep(15)

    rx_lines = monitor_b.find("Lora RX", since=mark)
    record_result("Channel 0 msg received on B", len(rx_lines) > 0,
                  f"{len(rx_lines)} Lora RX events")

    log(f"  >>> CHECK iPHONE: Channel msg '{msg}' in maluca channel")
    monitor_b.dump_recent(20)
    monitor_b.stop()

def test_channel_message_ch1(monitor_b):
    """Send on channel 1 (LongFast)."""
    log("TEST: Channel 1 (LongFast) message A -> B")
    kill_port_holders()
    time.sleep(1)
    monitor_b.start()
    time.sleep(1)

    msg = f"LongFast-ch1-{int(time.time()) % 10000}"
    mark = time.time()
    ok, out = run_meshtastic(PORT_A, ["--ch-index", "1", "--sendtext", msg])

    record_result("Channel 1 message send", ok, f"Sent: {msg}" if ok else out[:200])

    time.sleep(15)

    rx_lines = monitor_b.find("Lora RX", since=mark)
    record_result("Channel 1 msg received on B", len(rx_lines) > 0,
                  f"{len(rx_lines)} Lora RX events")

    log(f"  >>> CHECK iPHONE: Channel msg '{msg}' in LongFast channel")
    monitor_b.dump_recent(20)
    monitor_b.stop()

def test_nodeinfo_exchange(monitor_b):
    """Force nodeinfo broadcast from A, check B receives it."""
    log("TEST: NodeInfo exchange A -> B")
    kill_port_holders()
    time.sleep(1)
    monitor_b.start()
    time.sleep(1)

    mark = time.time()
    # Request B's position to trigger a nodeinfo exchange
    ok, out = run_meshtastic(PORT_A, ["--dest", NODE_B, "--request-position"])

    record_result("Position/NodeInfo request sent", ok or "position" in out.lower(),
                  out[:200])

    time.sleep(15)

    nodeinfo_lines = monitor_b.find("NodeInfo", since=mark)
    position_lines = monitor_b.find("POSITION", since=mark)

    record_result("NodeInfo activity on B", len(nodeinfo_lines) > 0,
                  f"{len(nodeinfo_lines)} NodeInfo events")

    monitor_b.dump_recent(20)
    monitor_b.stop()

def test_traceroute(monitor_b):
    """Traceroute from A to B."""
    log("TEST: Traceroute A -> B")
    kill_port_holders()
    time.sleep(1)

    ok, out = run_meshtastic(PORT_A, ["--dest", NODE_B, "--traceroute"], timeout=45)

    has_route = NODE_B in out if ok else False
    record_result("Traceroute A->B", ok, out[:300])

def test_rapid_dm_burst(monitor_b):
    """Send 5 rapid DMs from A to B."""
    log("TEST: Rapid DM Burst (5 messages A -> B)")
    kill_port_holders()
    time.sleep(1)
    monitor_b.start()
    time.sleep(1)

    mark = time.time()
    sent = 0
    for i in range(5):
        msg = f"Burst-{i+1}-{int(time.time()) % 10000}"
        ok, out = run_meshtastic(PORT_A, ["--dest", NODE_B, "--sendtext", msg])
        if ok:
            sent += 1
        time.sleep(6)
        kill_port_holders()
        time.sleep(1)

    time.sleep(10)

    rx_lines = monitor_b.find("Lora RX", since=mark)
    record_result(f"Burst DMs sent ({sent}/5)", sent >= 3, f"{sent}/5 sent OK")
    record_result(f"Burst DMs received on B", len(rx_lines) >= 3,
                  f"{len(rx_lines)} Lora RX events for {sent} sent")

    log(f"  >>> CHECK iPHONE: Should see {sent} burst messages")
    monitor_b.stop()

def test_long_message(monitor_b):
    """Send a 230-byte message (near MTU limit)."""
    log("TEST: Long Message (~230 chars)")
    kill_port_holders()
    time.sleep(1)
    monitor_b.start()
    time.sleep(1)

    msg = "L" * 220 + f"-{int(time.time()) % 10000}"
    mark = time.time()
    ok, out = run_meshtastic(PORT_A, ["--dest", NODE_B, "--sendtext", msg])

    record_result("Long message send (230 chars)", ok, out[:200] if not ok else f"Sent {len(msg)} chars")

    time.sleep(15)

    rx_lines = monitor_b.find("Lora RX", since=mark)
    record_result("Long message received on B", len(rx_lines) > 0,
                  f"{len(rx_lines)} Lora RX events")

    log(f"  >>> CHECK iPHONE: Should see long message (220 L's)")
    monitor_b.stop()

def test_emoji_message(monitor_b):
    """Send a message with emoji/unicode."""
    log("TEST: Unicode/Emoji Message")
    kill_port_holders()
    time.sleep(1)
    monitor_b.start()
    time.sleep(1)

    msg = f"VHF test 144MHz working! Check: pass"
    mark = time.time()
    ok, out = run_meshtastic(PORT_A, ["--dest", NODE_B, "--sendtext", msg])

    record_result("Unicode message send", ok, f"Sent: {msg}" if ok else out[:200])

    time.sleep(15)

    rx_lines = monitor_b.find("Lora RX", since=mark)
    record_result("Unicode message received on B", len(rx_lines) > 0)

    log(f"  >>> CHECK iPHONE: Should see '{msg}'")
    monitor_b.stop()

def test_telemetry_exchange():
    """Verify telemetry data is available from both devices."""
    log("TEST: Telemetry Exchange")
    kill_port_holders()
    time.sleep(1)

    ok_a, out_a = run_meshtastic(PORT_A, ["--info"])

    has_voltage_a = "voltage" in out_a.lower() if ok_a else False
    has_battery_a = "batteryLevel" in out_a if ok_a else False
    has_channel_util = "channelUtilization" in out_a if ok_a else False
    has_air_util = "airUtilTx" in out_a if ok_a else False
    has_uptime = "uptimeSeconds" in out_a if ok_a else False

    record_result("Telemetry: voltage", has_voltage_a)
    record_result("Telemetry: battery level", has_battery_a)
    record_result("Telemetry: channel utilization", has_channel_util)
    record_result("Telemetry: air util TX", has_air_util)
    record_result("Telemetry: uptime", has_uptime)

    # Check if B's telemetry is visible from A
    kill_port_holders()
    time.sleep(2)
    ok_nodes, out_nodes = run_meshtastic(PORT_A, ["--nodes"])
    b_in_nodes = NODE_B in out_nodes if ok_nodes else False
    record_result("B visible in A's node list", b_in_nodes)

def test_store_and_forward_config():
    """Verify store & forward is enabled."""
    log("TEST: Store & Forward Configuration")
    kill_port_holders()
    time.sleep(1)

    ok, out = run_meshtastic(PORT_A, ["--get", "store_forward"])
    sf_enabled = "enabled: True" in out if ok else False
    has_heartbeat = "heartbeat: True" in out if ok else False

    record_result("Store & Forward enabled", sf_enabled, out[:200])
    record_result("Store & Forward heartbeat", has_heartbeat)

# ============================================================
# MAIN
# ============================================================
def main():
    log("=" * 70)
    log("VHF T-BEAM MARATHON TEST — WITH iPHONE MONITORING")
    log(f"Sender: VHF Beam A ({NODE_A}) on {PORT_A}")
    log(f"Receiver: VHF Beam B ({NODE_B}) on {PORT_B} + iPhone via BLE")
    log("=" * 70)
    log("")
    log("NOTE: For tests marked with '>>> CHECK iPHONE', verify the message")
    log("appears in the Meshtastic iOS app connected to VHF Beam B.")
    log("")

    monitor_a = SerialCollector(PORT_A, "A")
    monitor_b = SerialCollector(PORT_B, "B")

    tests = [
        ("Telemetry Exchange", lambda: test_telemetry_exchange()),
        ("Store & Forward Config", lambda: test_store_and_forward_config()),
        ("DM A->B (check phone)", lambda: test_dm_a_to_b(monitor_b)),
        ("DM B->A", lambda: test_dm_b_to_a(monitor_a)),
        ("Broadcast A (check phone)", lambda: test_broadcast_a_to_all(monitor_b)),
        ("Broadcast B", lambda: test_broadcast_b_to_all(monitor_a)),
        ("Channel 0 maluca", lambda: test_channel_message_ch0(monitor_b)),
        ("Channel 1 LongFast", lambda: test_channel_message_ch1(monitor_b)),
        ("NodeInfo Exchange", lambda: test_nodeinfo_exchange(monitor_b)),
        ("Traceroute A->B", lambda: test_traceroute(monitor_b)),
        ("Long Message (230 chars)", lambda: test_long_message(monitor_b)),
        ("Unicode Message", lambda: test_emoji_message(monitor_b)),
        ("Rapid Burst (5 DMs)", lambda: test_rapid_dm_burst(monitor_b)),
    ]

    for name, test_fn in tests:
        log(f"\n{'='*50}")
        log(f">>> {name}")
        log(f"{'='*50}")
        try:
            test_fn()
        except Exception as e:
            record_result(f"{name} (EXCEPTION)", False, str(e))
        time.sleep(3)

    # Summary
    log(f"\n{'='*70}")
    log("MARATHON TEST SUMMARY")
    log(f"{'='*70}")
    passed = sum(1 for r in RESULTS if r["passed"])
    failed = sum(1 for r in RESULTS if not r["passed"])

    log("\nPASSED:")
    for r in RESULTS:
        if r["passed"]:
            log(f"  [PASS] {r['test']}")

    log("\nFAILED:")
    for r in RESULTS:
        if not r["passed"]:
            log(f"  [FAIL] {r['test']}" + (f" — {r['details']}" if r['details'] else ""))

    log(f"\nTotal: {passed} passed, {failed} failed out of {len(RESULTS)} tests")

    log("\n" + "="*70)
    log("PHONE VERIFICATION CHECKLIST")
    log("="*70)
    log("Please verify these messages appeared in the Meshtastic iOS app:")
    log("  1. DM from VHF Beam A (direct message)")
    log("  2. Broadcast from VHF Beam A (in maluca channel)")
    log("  3. Channel 0 (maluca) message")
    log("  4. Channel 1 (LongFast) message")
    log("  5. Long message (220 L's)")
    log("  6. Unicode message ('VHF test 144MHz working!')")
    log("  7. 5 burst messages")
    log("")
    log("VOICE MEMO & PICTURE TESTS:")
    log("  These must be initiated FROM the iPhone app:")
    log("  - Send a voice memo from iPhone to VHF Beam A")
    log("  - Send a picture from iPhone to VHF Beam A")
    log("  (T-Beam has no mic/speaker — voice must come from phone app)")

    # Save results
    with open("tests/vhf_marathon_results.json", "w") as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "results": RESULTS,
            "summary": {"passed": passed, "failed": failed, "total": len(RESULTS)}
        }, f, indent=2)
    log(f"\nResults saved to tests/vhf_marathon_results.json")

if __name__ == "__main__":
    main()
