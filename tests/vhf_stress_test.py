#!/usr/bin/env python3
"""
VHF T-Beam 500+ Message Stress Test
Sends a high volume of messages between devices A and B via meshtastic CLI.
Tracks send success/failure rates. Serial monitoring is NOT used to avoid
port contention with the CLI.

Phases:
  1. DM A->B: 150 messages
  2. DM B->A: 100 messages
  3. Broadcast: 50 from A, 50 from B
  4. Channel: 25 ch0, 25 ch1
  5. Mixed: 60 messages
  6. Long msgs: 20 near-MTU
  7. Rapid burst: 3x10
  8. Media notification: 2 messages
  Total: ~532 messages
"""

import subprocess
import time
import sys
import json
import os
import random
import string
from datetime import datetime

PORT_A = "/dev/cu.usbmodem101"   # VHF Beam A (!335e1be8)
PORT_B = "/dev/cu.usbmodem1101"  # VHF Beam B (!335e1bdc)
NODE_A = "!335e1be8"
NODE_B = "!335e1bdc"

stats = {
    "dm_a2b_sent": 0, "dm_a2b_ok": 0, "dm_a2b_fail": 0,
    "dm_b2a_sent": 0, "dm_b2a_ok": 0, "dm_b2a_fail": 0,
    "broadcast_a_sent": 0, "broadcast_a_ok": 0, "broadcast_a_fail": 0,
    "broadcast_b_sent": 0, "broadcast_b_ok": 0, "broadcast_b_fail": 0,
    "ch0_sent": 0, "ch0_ok": 0, "ch0_fail": 0,
    "ch1_sent": 0, "ch1_ok": 0, "ch1_fail": 0,
    "long_sent": 0, "long_ok": 0, "long_fail": 0,
    "media_sent": 0, "media_ok": 0, "media_fail": 0,
    "total_sent": 0, "total_ok": 0, "total_fail": 0,
    "send_errors": [],
    "consecutive_fails": 0,
    "max_consecutive_fails": 0,
}

LOG_FILE = None

def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    if LOG_FILE:
        LOG_FILE.write(line + "\n")
        LOG_FILE.flush()

def kill_port_holders():
    """Release serial ports held by previous meshtastic CLI instances."""
    for port in [PORT_A, PORT_B]:
        subprocess.run(
            f"lsof {port} 2>/dev/null | grep -v COMMAND | awk '{{print $2}}' | sort -u | xargs kill -9 2>/dev/null",
            shell=True, capture_output=True
        )
    time.sleep(1)

def run_meshtastic(port, args, timeout=20, retries=2):
    """Run meshtastic CLI command with retries on port errors."""
    cmd = ["meshtastic", "--port", port] + args

    for attempt in range(retries + 1):
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            output = result.stdout + result.stderr
            ok = result.returncode == 0
            # Check for known error strings
            if "OS Error" in output or "Errno" in output or "couldn't be opened" in output:
                if attempt < retries:
                    kill_port_holders()
                    time.sleep(3)
                    continue
                ok = False
            return ok, output.strip()
        except subprocess.TimeoutExpired:
            if attempt < retries:
                kill_port_holders()
                time.sleep(3)
                continue
            return False, "TIMEOUT"
        except Exception as e:
            if attempt < retries:
                kill_port_holders()
                time.sleep(3)
                continue
            return False, str(e)

    return False, "ALL_RETRIES_FAILED"


def track_result(stat_prefix, ok, error_msg=""):
    """Update stats counters."""
    stats["total_sent"] += 1
    stats[f"{stat_prefix}_sent"] += 1

    if ok:
        stats[f"{stat_prefix}_ok"] += 1
        stats["total_ok"] += 1
        stats["consecutive_fails"] = 0
    else:
        stats[f"{stat_prefix}_fail"] += 1
        stats["total_fail"] += 1
        stats["consecutive_fails"] += 1
        stats["max_consecutive_fails"] = max(stats["max_consecutive_fails"], stats["consecutive_fails"])
        if error_msg:
            stats["send_errors"].append(f"#{stats['total_sent']} {stat_prefix}: {error_msg[:100]}")


def phase_dm_stress(count, direction="a2b"):
    """Send `count` DMs in one direction."""
    from_port = PORT_A if direction == "a2b" else PORT_B
    to_node = NODE_B if direction == "a2b" else NODE_A
    from_label = "A" if direction == "a2b" else "B"
    to_label = "B" if direction == "a2b" else "A"
    stat_prefix = f"dm_{direction}"

    log(f"  Sending {count} DMs {from_label} -> {to_label}...")

    batch_ok = 0
    batch_fail = 0

    for i in range(count):
        msg = f"DM-{direction}-{i+1:04d}-{int(time.time()) % 10000}"
        ok, out = run_meshtastic(from_port, ["--dest", to_node, "--sendtext", msg])
        track_result(stat_prefix, ok, out if not ok else "")

        if ok:
            batch_ok += 1
        else:
            batch_fail += 1

        # Settle time between messages
        time.sleep(3)

        # Progress every 25 messages
        if (i + 1) % 25 == 0:
            total = stats["total_sent"]
            rate = stats["total_ok"] / max(stats["total_sent"], 1) * 100
            log(f"    Progress: {i+1}/{count} (batch: {batch_ok}ok/{batch_fail}fail) | Overall: {total} sent, {rate:.0f}% success")

    return batch_ok, batch_fail


def phase_broadcast_stress(count, from_port, from_label, stat_prefix, ch_index=None):
    """Send `count` broadcasts."""
    ch_str = f" ch{ch_index}" if ch_index is not None else ""
    log(f"  Sending {count} broadcasts from {from_label}{ch_str}...")

    batch_ok = 0
    batch_fail = 0

    for i in range(count):
        msg = f"BC-{from_label}-{stats['total_sent']+1}-{int(time.time()) % 10000}"
        args = ["--sendtext", msg]
        if ch_index is not None:
            args = ["--ch-index", str(ch_index)] + args

        ok, out = run_meshtastic(from_port, args)
        track_result(stat_prefix, ok, out if not ok else "")

        if ok:
            batch_ok += 1
        else:
            batch_fail += 1

        time.sleep(3)

        if (i + 1) % 25 == 0:
            total = stats["total_sent"]
            rate = stats["total_ok"] / max(stats["total_sent"], 1) * 100
            log(f"    Progress: {i+1}/{count} (batch: {batch_ok}ok/{batch_fail}fail) | Overall: {total} sent, {rate:.0f}% success")

    return batch_ok, batch_fail


def phase_mixed_traffic(count):
    """Send a mix of DMs, broadcasts, and channel messages."""
    log(f"  Sending {count} mixed messages (DMs, broadcasts, channel msgs)...")

    batch_ok = 0
    batch_fail = 0
    msg_types = ["dm_a2b", "dm_b2a", "broadcast_a", "broadcast_b", "ch0", "ch1"]

    for i in range(count):
        msg_type = msg_types[i % len(msg_types)]
        msg = f"MIX-{msg_type}-{i+1:04d}-{int(time.time()) % 10000}"
        ok = False
        out = ""

        if msg_type == "dm_a2b":
            ok, out = run_meshtastic(PORT_A, ["--dest", NODE_B, "--sendtext", msg])
            track_result("dm_a2b", ok, out if not ok else "")
        elif msg_type == "dm_b2a":
            ok, out = run_meshtastic(PORT_B, ["--dest", NODE_A, "--sendtext", msg])
            track_result("dm_b2a", ok, out if not ok else "")
        elif msg_type == "broadcast_a":
            ok, out = run_meshtastic(PORT_A, ["--sendtext", msg])
            track_result("broadcast_a", ok, out if not ok else "")
        elif msg_type == "broadcast_b":
            ok, out = run_meshtastic(PORT_B, ["--sendtext", msg])
            track_result("broadcast_b", ok, out if not ok else "")
        elif msg_type == "ch0":
            ok, out = run_meshtastic(PORT_A, ["--ch-index", "0", "--sendtext", msg])
            track_result("ch0", ok, out if not ok else "")
        elif msg_type == "ch1":
            ok, out = run_meshtastic(PORT_A, ["--ch-index", "1", "--sendtext", msg])
            track_result("ch1", ok, out if not ok else "")

        if ok:
            batch_ok += 1
        else:
            batch_fail += 1

        time.sleep(3)

        if (i + 1) % 25 == 0:
            total = stats["total_sent"]
            rate = stats["total_ok"] / max(stats["total_sent"], 1) * 100
            log(f"    Progress: {i+1}/{count} (batch: {batch_ok}ok/{batch_fail}fail) | Overall: {total} sent, {rate:.0f}% success")

    return batch_ok, batch_fail


def phase_long_messages(count):
    """Send near-MTU messages."""
    log(f"  Sending {count} long messages (~220 chars)...")

    batch_ok = 0
    batch_fail = 0

    for i in range(count):
        payload = ''.join(random.choices(string.ascii_uppercase, k=210))
        msg = f"{payload}-{i+1:04d}"

        ok, out = run_meshtastic(PORT_A, ["--dest", NODE_B, "--sendtext", msg])
        track_result("long", ok, out if not ok else "")

        if ok:
            batch_ok += 1
        else:
            batch_fail += 1

        time.sleep(5)  # Longer settle for big messages

        if (i + 1) % 10 == 0:
            log(f"    Progress: {i+1}/{count} ({batch_ok}ok/{batch_fail}fail)")

    return batch_ok, batch_fail


def phase_rapid_burst(burst_size=10, inter_msg_delay=1):
    """Send a rapid burst with minimal inter-message delay."""
    log(f"  Rapid burst: {burst_size} messages with {inter_msg_delay}s delay...")

    batch_ok = 0
    batch_fail = 0

    for i in range(burst_size):
        msg = f"BURST-{i+1:03d}-{int(time.time()) % 10000}"

        ok, out = run_meshtastic(PORT_A, ["--dest", NODE_B, "--sendtext", msg])
        track_result("dm_a2b", ok, out if not ok else "")

        if ok:
            batch_ok += 1
        else:
            batch_fail += 1

        time.sleep(inter_msg_delay)

    log(f"    Burst result: {batch_ok}/{burst_size} sent OK")
    return batch_ok, batch_fail


def phase_media_test():
    """Send media notification messages."""
    log("  Testing media notification messages...")

    test_dir = os.path.dirname(os.path.abspath(__file__))
    wav_path = os.path.join(test_dir, "test_voice.wav")
    png_path = os.path.join(test_dir, "test_image.png")

    for fpath, ftype in [(wav_path, "voice memo"), (png_path, "image")]:
        if not os.path.exists(fpath):
            log(f"    [SKIP] {ftype} file not found: {fpath}")
            continue

        fsize = os.path.getsize(fpath)
        msg = f"[MEDIA-TEST] {ftype} ({fsize}B) A->B ts={int(time.time()) % 10000}"

        ok, out = run_meshtastic(PORT_A, ["--dest", NODE_B, "--sendtext", msg])
        track_result("media", ok, out if not ok else "")

        if ok:
            log(f"    [OK] {ftype} notification sent ({fsize} bytes)")
        else:
            log(f"    [FAIL] {ftype} notification: {out[:100]}")

        time.sleep(5)

    log("    NOTE: Actual media must be sent from iPhone Meshtastic app.")


def print_summary():
    """Print detailed summary."""
    log("")
    log("=" * 70)
    log("STRESS TEST COMPLETE — DETAILED RESULTS")
    log("=" * 70)

    total = stats['total_sent']
    ok = stats['total_ok']
    fail = stats['total_fail']
    rate = ok / max(total, 1) * 100

    log(f"\n  Total messages attempted: {total}")
    log(f"  Total send OK:           {ok}")
    log(f"  Total send FAIL:         {fail}")
    log(f"  Send success rate:       {rate:.1f}%")
    log(f"  Max consecutive fails:   {stats['max_consecutive_fails']}")

    log(f"\n  Breakdown:")
    log(f"    DM A->B:      {stats['dm_a2b_ok']}/{stats['dm_a2b_sent']} ok ({stats['dm_a2b_fail']} fail)")
    log(f"    DM B->A:      {stats['dm_b2a_ok']}/{stats['dm_b2a_sent']} ok ({stats['dm_b2a_fail']} fail)")
    log(f"    Broadcast A:  {stats['broadcast_a_ok']}/{stats['broadcast_a_sent']} ok ({stats['broadcast_a_fail']} fail)")
    log(f"    Broadcast B:  {stats['broadcast_b_ok']}/{stats['broadcast_b_sent']} ok ({stats['broadcast_b_fail']} fail)")
    log(f"    Channel 0:    {stats['ch0_ok']}/{stats['ch0_sent']} ok ({stats['ch0_fail']} fail)")
    log(f"    Channel 1:    {stats['ch1_ok']}/{stats['ch1_sent']} ok ({stats['ch1_fail']} fail)")
    log(f"    Long msgs:    {stats['long_ok']}/{stats['long_sent']} ok ({stats['long_fail']} fail)")
    log(f"    Media:        {stats['media_ok']}/{stats['media_sent']} ok ({stats['media_fail']} fail)")

    if stats['send_errors']:
        log(f"\n  First 20 errors:")
        for err in stats['send_errors'][:20]:
            log(f"    {err}")

    log(f"\n  Phone verification checklist:")
    log(f"    - DMs from VHF Beam A should appear in iPhone Meshtastic app")
    log(f"    - Broadcasts should appear in channel view")
    log(f"    - Channel 0/1 messages should appear in respective channels")
    log(f"    - Send voice memo FROM iPhone to VHF Beam A (manual test)")
    log(f"    - Send picture FROM iPhone to VHF Beam A (manual test)")


def main():
    global LOG_FILE

    os.makedirs("tests", exist_ok=True)
    LOG_FILE = open("tests/vhf_stress_log.txt", "w")

    log("=" * 70)
    log("VHF T-BEAM 500+ MESSAGE STRESS TEST")
    log(f"Device A: {NODE_A} on {PORT_A}")
    log(f"Device B: {NODE_B} on {PORT_B} (iPhone connected via BLE)")
    log(f"Started: {datetime.now().isoformat()}")
    log("=" * 70)

    # Verify devices are accessible
    log("\nPre-flight: checking devices...")
    kill_port_holders()
    time.sleep(2)

    ok_a, out_a = run_meshtastic(PORT_A, ["--info"], timeout=15)
    if not ok_a:
        log(f"[FATAL] Cannot connect to Device A on {PORT_A}: {out_a[:200]}")
        return
    log(f"  Device A: OK")

    kill_port_holders()
    time.sleep(2)

    ok_b, out_b = run_meshtastic(PORT_B, ["--info"], timeout=15)
    if not ok_b:
        log(f"[FATAL] Cannot connect to Device B on {PORT_B}: {out_b[:200]}")
        return
    log(f"  Device B: OK")

    kill_port_holders()
    time.sleep(2)

    # ========================================
    # PHASE 1: DM Stress A -> B (150 messages)
    # ========================================
    log(f"\n{'='*60}")
    log("PHASE 1: DM Stress A -> B (150 messages)")
    log(f"{'='*60}")
    ok1, fail1 = phase_dm_stress(150, direction="a2b")
    log(f"  Phase 1 result: {ok1} ok, {fail1} fail")
    time.sleep(5)

    # ========================================
    # PHASE 2: DM Stress B -> A (100 messages)
    # ========================================
    log(f"\n{'='*60}")
    log("PHASE 2: DM Stress B -> A (100 messages)")
    log(f"{'='*60}")
    ok2, fail2 = phase_dm_stress(100, direction="b2a")
    log(f"  Phase 2 result: {ok2} ok, {fail2} fail")
    time.sleep(5)

    # ========================================
    # PHASE 3: Broadcast Stress (50 each direction)
    # ========================================
    log(f"\n{'='*60}")
    log("PHASE 3: Broadcast Stress (50 from A, 50 from B)")
    log(f"{'='*60}")
    ok3a, fail3a = phase_broadcast_stress(50, PORT_A, "A", "broadcast_a")
    ok3b, fail3b = phase_broadcast_stress(50, PORT_B, "B", "broadcast_b")
    log(f"  Phase 3 result: A={ok3a}ok/{fail3a}fail, B={ok3b}ok/{fail3b}fail")
    time.sleep(5)

    # ========================================
    # PHASE 4: Channel Messages (25 ch0, 25 ch1)
    # ========================================
    log(f"\n{'='*60}")
    log("PHASE 4: Channel Messages (25 on ch0, 25 on ch1)")
    log(f"{'='*60}")
    ok4a, fail4a = phase_broadcast_stress(25, PORT_A, "A", "ch0", ch_index=0)
    ok4b, fail4b = phase_broadcast_stress(25, PORT_A, "A", "ch1", ch_index=1)
    log(f"  Phase 4 result: ch0={ok4a}ok/{fail4a}fail, ch1={ok4b}ok/{fail4b}fail")
    time.sleep(5)

    # ========================================
    # PHASE 5: Mixed Traffic (60 messages)
    # ========================================
    log(f"\n{'='*60}")
    log("PHASE 5: Mixed Traffic (60 messages — DMs + broadcasts + channels)")
    log(f"{'='*60}")
    ok5, fail5 = phase_mixed_traffic(60)
    log(f"  Phase 5 result: {ok5} ok, {fail5} fail")
    time.sleep(5)

    # ========================================
    # PHASE 6: Long Messages (20 near-MTU)
    # ========================================
    log(f"\n{'='*60}")
    log("PHASE 6: Long Messages (~220 chars, 20 messages)")
    log(f"{'='*60}")
    ok6, fail6 = phase_long_messages(20)
    log(f"  Phase 6 result: {ok6} ok, {fail6} fail")
    time.sleep(5)

    # ========================================
    # PHASE 7: Rapid Bursts (3 bursts of 10)
    # ========================================
    log(f"\n{'='*60}")
    log("PHASE 7: Rapid Bursts (3 bursts of 10 with 1s delay)")
    log(f"{'='*60}")
    for burst_num in range(3):
        log(f"  Burst {burst_num+1}/3:")
        ok7, fail7 = phase_rapid_burst(burst_size=10, inter_msg_delay=1)
        log(f"    Result: {ok7} ok, {fail7} fail")
        time.sleep(10)  # Cooldown between bursts

    # ========================================
    # PHASE 8: Media Test
    # ========================================
    log(f"\n{'='*60}")
    log("PHASE 8: Media Test (voice memo + picture notifications)")
    log(f"{'='*60}")
    phase_media_test()

    # Print summary
    print_summary()

    # Save JSON results
    results_path = "tests/vhf_stress_results.json"
    with open(results_path, "w") as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "stats": {k: v for k, v in stats.items() if k != "send_errors"},
            "errors": stats["send_errors"][:50],
            "summary": {
                "total_sent": stats["total_sent"],
                "total_ok": stats["total_ok"],
                "total_fail": stats["total_fail"],
                "success_rate": f"{stats['total_ok']/max(stats['total_sent'],1)*100:.1f}%",
            }
        }, f, indent=2)
    log(f"\nResults saved to {results_path}")
    log(f"Full log saved to tests/vhf_stress_log.txt")

    if LOG_FILE:
        LOG_FILE.close()

if __name__ == "__main__":
    main()
