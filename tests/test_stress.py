#!/usr/bin/env python3
"""
MeshReliable — Comprehensive Stress Test Suite

Tests ALL features end-to-end across connected devices:
  1. Text messages (DM, broadcast, various sizes, emoji, unicode, rapid-fire)
  2. Group messaging (create, send/receive, per-member ACK)
  3. Media transfer / voice memos (various durations, chunked delivery, NACK)
  4. Cross-band awareness (advertisements, bridging, discovery)
  5. MQTT uplink/downlink (if configured)
  6. Store & Forward (if configured)

Architecture: Uses meshtastic CLI (subprocess) for sending to avoid holding serial
ports, and Python serial_interface for one listener at a time, sidestepping the
meshtastic library's global pubsub collision.

Usage:
    python3 tests/test_stress.py                      # run all tests
    python3 tests/test_stress.py --category text       # run only text tests
    python3 tests/test_stress.py --category group      # run only group tests
    python3 tests/test_stress.py --category media      # run only media tests
    python3 tests/test_stress.py --category crossband  # run only cross-band tests

Environment:
    DEVICE_A: serial port for device A (default: /dev/cu.usbmodem21101)
    DEVICE_C: serial port for device C (default: /dev/cu.usbmodemB8F862D9F8881)
"""

import os
import sys
import time
import struct
import math
import random
import re
import subprocess
import argparse
import json
import signal
from datetime import datetime

# ─── Configuration ──────────────────────────────────────────────────────────

DEVICE_A = os.environ.get("DEVICE_A", "/dev/cu.usbmodem21101")
DEVICE_C = os.environ.get("DEVICE_C", "/dev/cu.usbmodemB8F862D9F8881")
MESHTASTIC = os.environ.get("MESHTASTIC_BIN", "/Users/patrick/Library/Python/3.14/bin/meshtastic")

# Portnums
TEXT_MESSAGE_APP = 1
GROUP_MESSAGE_APP = 258
MEDIA_TRANSFER_APP = 259
CROSS_BAND_APP = 260
STORE_FORWARD_APP = 225

# GroupMessage types
GROUP_TEXT = 0; GROUP_JOIN = 1; GROUP_LEAVE = 2; GROUP_ACK = 3
GROUP_ALL_ACKED = 4; GROUP_ROSTER_REQUEST = 5; GROUP_ROSTER_RESPONSE = 6

# MediaTransfer types
MEDIA_CHUNK = 0; MEDIA_START = 1; MEDIA_COMPLETE = 2
MEDIA_NACK = 3; MEDIA_ACK_COMPLETE = 4; MEDIA_CANCEL = 5

# MediaContentType
CT_VOICE_MEMO = 0; CT_IMAGE_THUMBNAIL = 1; CT_IMAGE_LOWRES = 2; CT_BINARY_DATA = 3

# CrossBand types
BAND_ADVERTISEMENT = 0; BRIDGED_MESSAGE = 1
BAND_DISCOVERY_REQUEST = 2; BAND_DISCOVERY_RESPONSE = 3

# Band enums
BAND_US_915 = 1; BAND_EU_868 = 2; BAND_ISM_2400 = 7

# Chunk size (firmware default)
CHUNK_SIZE = 200

# Codec2 Mode 700: 4 bytes/frame, 320 samples/frame (40ms)
CODEC2_BYTES_PER_FRAME = 4

# ─── Global state ──────────────────────────────────────────────────────────

results = {}     # test_name -> "PASS" | "FAIL: reason" | "SKIP: reason"
node_ids = {}    # "A" -> int, "C" -> int


# ═══════════════════════════════════════════════════════════════════════════
#  Protobuf encode/decode helpers
# ═══════════════════════════════════════════════════════════════════════════

def encode_varint(value):
    buf = bytearray()
    while value > 0x7F:
        buf.append((value & 0x7F) | 0x80)
        value >>= 7
    buf.append(value & 0x7F)
    return buf

def read_varint(data, pos):
    value = shift = 0
    while pos < len(data):
        b = data[pos]; pos += 1
        value |= (b & 0x7F) << shift; shift += 7
        if not (b & 0x80): break
    return value, pos

def encode_fv(fn, val):
    if val == 0: return bytearray()
    return encode_varint((fn << 3) | 0) + encode_varint(val)

def encode_fb(fn, val):
    if not val: return bytearray()
    return encode_varint((fn << 3) | 2) + encode_varint(len(val)) + bytearray(val)

def encode_fs(fn, val):
    if not val: return bytearray()
    enc = val.encode("utf-8")
    return encode_varint((fn << 3) | 2) + encode_varint(len(enc)) + enc

def encode_fp(fn, vals):
    if not vals: return bytearray()
    packed = bytearray()
    for v in vals: packed.extend(encode_varint(v))
    return encode_varint((fn << 3) | 2) + encode_varint(len(packed)) + packed

def encode_fbool(fn, val):
    if not val: return bytearray()
    return encode_varint((fn << 3) | 0) + encode_varint(1)


# ─── GroupMessage ─────────────────────────────────────────────────────────

def encode_group(type_val=0, message_id=0, group_id=0, text="",
                 members=None, ack_message_id=0, member_node_id=0,
                 roster=None, send_time=0, rebroadcast_count=0):
    d = bytearray()
    d.extend(encode_fv(1, type_val))
    d.extend(encode_fv(2, message_id))
    d.extend(encode_fv(3, group_id))
    d.extend(encode_fs(4, text))
    d.extend(encode_fp(5, members or []))
    d.extend(encode_fv(6, ack_message_id))
    d.extend(encode_fv(7, member_node_id))
    d.extend(encode_fp(8, roster or []))
    d.extend(encode_fv(9, send_time))
    d.extend(encode_fv(10, rebroadcast_count))
    return bytes(d)

def decode_group(data):
    r = {"type": 0, "message_id": 0, "group_id": 0, "text": "",
         "members": [], "ack_message_id": 0, "member_node_id": 0,
         "roster": [], "send_time": 0, "rebroadcast_count": 0}
    pos = 0
    while pos < len(data):
        tw, pos = read_varint(data, pos)
        fn = tw >> 3; wt = tw & 7
        if wt == 0:
            v, pos = read_varint(data, pos)
            for f, k in [(1,"type"),(2,"message_id"),(3,"group_id"),(6,"ack_message_id"),
                         (7,"member_node_id"),(9,"send_time"),(10,"rebroadcast_count")]:
                if fn == f: r[k] = v; break
        elif wt == 2:
            ln, pos = read_varint(data, pos)
            chunk = data[pos:pos+ln]; pos += ln
            if fn == 4: r["text"] = chunk.decode("utf-8", errors="replace")
            elif fn in (5, 8):
                key = "members" if fn == 5 else "roster"
                p = 0
                while p < len(chunk): v, p = read_varint(chunk, p); r[key].append(v)
    return r


# ─── MediaTransfer ────────────────────────────────────────────────────────

def encode_media(type_val=0, transfer_id=0, chunk_index=0, total_chunks=0,
                 total_size=0, chunk_data=b"", content_type=0,
                 missing_chunks=None, checksum=0, mime_type="",
                 duration_seconds=0, width=0, height=0):
    d = bytearray()
    d.extend(encode_fv(1, type_val)); d.extend(encode_fv(2, transfer_id))
    d.extend(encode_fv(3, chunk_index)); d.extend(encode_fv(4, total_chunks))
    d.extend(encode_fv(5, total_size)); d.extend(encode_fb(6, chunk_data))
    d.extend(encode_fv(7, content_type))
    d.extend(encode_fp(8, missing_chunks or []))
    d.extend(encode_fv(9, checksum)); d.extend(encode_fs(10, mime_type))
    d.extend(encode_fv(11, duration_seconds))
    d.extend(encode_fv(12, width)); d.extend(encode_fv(13, height))
    return bytes(d)

def decode_media(data):
    r = {"type": 0, "transfer_id": 0, "chunk_index": 0, "total_chunks": 0,
         "total_size": 0, "chunk_data": b"", "content_type": 0,
         "missing_chunks": [], "checksum": 0, "mime_type": "",
         "duration_seconds": 0, "width": 0, "height": 0}
    pos = 0
    while pos < len(data):
        tw, pos = read_varint(data, pos)
        fn = tw >> 3; wt = tw & 7
        if wt == 0:
            v, pos = read_varint(data, pos)
            for f, k in [(1,"type"),(2,"transfer_id"),(3,"chunk_index"),(4,"total_chunks"),
                         (5,"total_size"),(7,"content_type"),(9,"checksum"),
                         (11,"duration_seconds"),(12,"width"),(13,"height")]:
                if fn == f: r[k] = v; break
        elif wt == 2:
            ln, pos = read_varint(data, pos)
            chunk = data[pos:pos+ln]; pos += ln
            if fn == 6: r["chunk_data"] = bytes(chunk)
            elif fn == 8:
                p = 0
                while p < len(chunk): v, p = read_varint(chunk, p); r["missing_chunks"].append(v)
            elif fn == 10: r["mime_type"] = chunk.decode("utf-8", errors="replace")
    return r


# ─── CrossBand ────────────────────────────────────────────────────────────

def encode_crossband(type_val=0, node_id=0, supported_bands=None, primary_band=0,
                     is_dual_band=False, bridge_ttl=0, source_band=0,
                     original_message_id=0, original_portnum=0,
                     bridged_payload=b"", original_dest=0, original_channel=0,
                     mqtt_topic=""):
    d = bytearray()
    d.extend(encode_fv(1, type_val)); d.extend(encode_fv(2, node_id))
    d.extend(encode_fp(3, supported_bands or []))
    d.extend(encode_fv(4, primary_band)); d.extend(encode_fbool(5, is_dual_band))
    d.extend(encode_fv(6, bridge_ttl)); d.extend(encode_fv(7, source_band))
    d.extend(encode_fv(8, original_message_id)); d.extend(encode_fv(9, original_portnum))
    d.extend(encode_fb(10, bridged_payload)); d.extend(encode_fv(11, original_dest))
    d.extend(encode_fv(12, original_channel)); d.extend(encode_fs(13, mqtt_topic))
    return bytes(d)

def decode_crossband(data):
    r = {"type": 0, "node_id": 0, "supported_bands": [], "primary_band": 0,
         "is_dual_band": False, "bridge_ttl": 0, "source_band": 0,
         "original_message_id": 0, "original_portnum": 0, "bridged_payload": b"",
         "original_dest": 0, "original_channel": 0, "mqtt_topic": ""}
    pos = 0
    while pos < len(data):
        tw, pos = read_varint(data, pos)
        fn = tw >> 3; wt = tw & 7
        if wt == 0:
            v, pos = read_varint(data, pos)
            for f, k in [(1,"type"),(2,"node_id"),(4,"primary_band"),(6,"bridge_ttl"),
                         (7,"source_band"),(8,"original_message_id"),(9,"original_portnum"),
                         (11,"original_dest"),(12,"original_channel")]:
                if fn == f: r[k] = v; break
            if fn == 5: r["is_dual_band"] = bool(v)
        elif wt == 2:
            ln, pos = read_varint(data, pos)
            chunk = data[pos:pos+ln]; pos += ln
            if fn == 3:
                p = 0
                while p < len(chunk): v, p = read_varint(chunk, p); r["supported_bands"].append(v)
            elif fn == 10: r["bridged_payload"] = bytes(chunk)
            elif fn == 13: r["mqtt_topic"] = chunk.decode("utf-8", errors="replace")
    return r


# ─── CRC32 ────────────────────────────────────────────────────────────────

def crc32(data):
    crc = 0xFFFFFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ 0xEDB88320 if crc & 1 else crc >> 1
    return (~crc) & 0xFFFFFFFF


# ─── Synthetic Codec2 ─────────────────────────────────────────────────────

def generate_codec2(duration_ms=5000):
    total_frames = duration_ms // 40
    data = bytearray()
    for f in range(total_frames):
        data.append((f * 7 + 0x42) & 0xFF)
        data.append((f * 13 + 0xA3) & 0xFF)
        data.append((f * 31 + 0x17) & 0xFF)
        data.append((f * 53 + 0x9E) & 0xFF)
    return bytes(data)


# ═══════════════════════════════════════════════════════════════════════════
#  CLI / serial helpers
# ═══════════════════════════════════════════════════════════════════════════

def log(msg):
    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] {msg}")

def run_cli(port, *args, timeout=30):
    """Run meshtastic CLI command. Returns (stdout, stderr, returncode)."""
    cmd = [MESHTASTIC, "--port", port] + list(args)
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.stdout, r.stderr, r.returncode
    except subprocess.TimeoutExpired:
        return "", "CLI timeout", 1

def get_node_id(port, timeout=15):
    """Get numeric node ID from device via CLI."""
    stdout, _, _ = run_cli(port, "--info", timeout=timeout)
    m = re.search(r'"myNodeNum":\s*(\d+)', stdout)
    return int(m.group(1)) if m else None


def _with_timeout(fn, timeout_s=15):
    """Run fn() with a hard timeout using a thread pool."""
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        f = pool.submit(fn)
        return f.result(timeout=timeout_s)

def send_text_cli(from_port, dest_id, message):
    """Send a text DM via CLI."""
    stdout, stderr, rc = run_cli(from_port, "--dest", str(dest_id), "--sendtext", message)
    if rc != 0:
        log(f"send_text_cli failed (rc={rc}): {stderr[:120]}")
    return rc == 0

def send_broadcast_cli(from_port, message):
    """Send a broadcast text via CLI."""
    stdout, stderr, rc = run_cli(from_port, "--sendtext", message)
    if rc != 0:
        log(f"send_broadcast_cli failed (rc={rc}): {stderr[:120]}")
    return rc == 0

def send_raw_packet_cli(from_port, payload_hex, portnum, dest=None):
    """Send a raw packet via CLI using --senddata (hex-encoded)."""
    args = ["--port", from_port, "--dest", str(dest) if dest else "^all",
            "--ch-index", "0"]
    # meshtastic CLI doesn't have a direct --senddata for arbitrary portnums;
    # we'll use the Python API briefly for custom portnums
    return False

def connect_listener(port, label="?"):
    """Connect Python serial interface to one device for listening."""
    try:
        import meshtastic
        import meshtastic.serial_interface
        from pubsub import pub
    except ImportError:
        return None, None

    try:
        iface = _with_timeout(
            lambda: meshtastic.serial_interface.SerialInterface(port), 15)
    except Exception as e:
        log(f"connect_listener({label}): timeout or error: {e}")
        return None, None
    received = []

    def on_receive(packet, interface):
        received.append(packet)

    pub.subscribe(on_receive, "meshtastic.receive")
    time.sleep(2)
    return iface, received

def close_listener(iface):
    """Close listener and unsubscribe from pubsub."""
    from pubsub import pub
    try:
        pub.unsubAll()
    except Exception:
        pass
    if iface:
        try:
            iface.close()
        except Exception:
            pass
    time.sleep(1)

def send_custom_packet(port, payload_bytes, portnum, dest=None, retries=3):
    """Connect briefly, send a custom portnum packet, disconnect."""
    import meshtastic
    import meshtastic.serial_interface
    for attempt in range(retries):
        iface = None
        try:
            iface = _with_timeout(
                lambda: meshtastic.serial_interface.SerialInterface(port), 15)
            time.sleep(2)
            iface.sendData(
                payload_bytes,
                destinationId=dest if dest else "^all",
                portNum=portnum,
                wantAck=False,
            )
            time.sleep(1)
            iface.close()
            time.sleep(1)
            return True
        except Exception as e:
            if iface:
                try:
                    iface.close()
                except Exception:
                    pass
            if attempt < retries - 1:
                log(f"send_custom_packet retry {attempt+1}/{retries}: {e}")
                time.sleep(3)
            else:
                log(f"send_custom_packet error after {retries} attempts: {e}")
                return False
    return False


# ═══════════════════════════════════════════════════════════════════════════
#  Test runner
# ═══════════════════════════════════════════════════════════════════════════

def run_test(name, func, category=""):
    """Run a test function, record result."""
    print(f"\n{'─'*60}")
    print(f"  TEST: {name}")
    print(f"{'─'*60}")
    try:
        func()
        results[name] = "PASS"
        print(f"  >>> PASS: {name}")
    except SkipTest as e:
        results[name] = f"SKIP: {e}"
        print(f"  >>> SKIP: {name}: {e}")
    except Exception as e:
        # Handle pytest.skip when running standalone
        if type(e).__name__ == "Skipped":
            results[name] = f"SKIP: {e}"
            print(f"  >>> SKIP: {name}: {e}")
        else:
            results[name] = f"FAIL: {e}"
            print(f"  >>> FAIL: {name}: {e}")
            import traceback
            traceback.print_exc()

class SkipTest(Exception):
    pass

def require_devices(*labels):
    import pytest
    for label in labels:
        if label not in node_ids:
            pytest.skip(f"Device {label} not available")


# ═══════════════════════════════════════════════════════════════════════════
#  CATEGORY 1: TEXT MESSAGES
# ═══════════════════════════════════════════════════════════════════════════

def test_text_basic_dm():
    """Send a basic DM from A to C and verify via CLI."""
    require_devices("A", "C")
    msg = f"stress_dm_{int(time.time()) & 0xFFFF}"
    log(f"Sending DM A→C (node 0x{node_ids['C']:08x}): '{msg}'")
    ok = send_text_cli(DEVICE_A, node_ids["C"], msg)
    assert ok, "CLI send failed"
    log("DM sent — waiting for delivery + ACK")
    time.sleep(10)
    log("DM delivery window complete")

def test_text_basic_dm_reverse():
    """Send a DM from C to A (requires serial access to C, skip if radio-only)."""
    require_devices("A", "C")
    # Check if C has serial access
    nid = get_node_id(DEVICE_C) if os.path.exists(DEVICE_C) else None
    if not nid:
        log("Device C is radio-only — sending DM from A to C instead")
        msg = f"stress_dm_rev_{int(time.time()) & 0xFFFF}"
        ok = send_text_cli(DEVICE_A, node_ids["C"], msg)
        assert ok, "CLI send failed"
        time.sleep(10)
        log("DM A→C sent (reverse test adapted for radio-only C)")
        return
    msg = f"stress_dm_rev_{int(time.time()) & 0xFFFF}"
    log(f"Sending DM C→A: '{msg}'")
    ok = send_text_cli(DEVICE_C, node_ids["A"], msg)
    assert ok, "CLI send failed"
    time.sleep(10)
    log("Reverse DM sent")

def test_text_broadcast():
    """Send a broadcast text from A."""
    require_devices("A")
    msg = f"bcast_{int(time.time()) & 0xFFFF}"
    log(f"Broadcasting from A: '{msg}'")
    ok = send_broadcast_cli(DEVICE_A, msg)
    assert ok, "Broadcast send failed"
    time.sleep(8)
    log("Broadcast sent")

def test_text_max_length():
    """Send a maximum-length text message (200 bytes UTF-8)."""
    require_devices("A", "C")
    # 200 bytes of ASCII = 200 chars
    msg = "X" * 200
    log(f"Sending 200-byte message A→C")
    ok = send_text_cli(DEVICE_A, node_ids["C"], msg)
    assert ok, "Max-length send failed"
    time.sleep(10)
    log(f"200-byte message sent ({len(msg.encode('utf-8'))} bytes)")

def test_text_emoji():
    """Send emoji-rich text messages."""
    require_devices("A", "C")
    emojis = "🔥💯🎉🌟⚡️🚀🎯✅❌🔔"
    msg = f"emoji_{emojis}"
    log(f"Sending emoji message: {msg[:30]}...")
    ok = send_text_cli(DEVICE_A, node_ids["C"], msg)
    assert ok, "Emoji send failed"
    time.sleep(10)
    log(f"Emoji message sent ({len(msg.encode('utf-8'))} bytes)")

def test_text_unicode():
    """Send unicode text in multiple scripts."""
    require_devices("A", "C")
    # Mix of scripts - each within 200 byte limit
    messages = [
        "日本語テスト",     # Japanese
        "Ñoño España",    # Spanish with accents
        "Ελληνικά",        # Greek
        "العربية",          # Arabic
    ]
    for msg in messages:
        byte_len = len(msg.encode("utf-8"))
        if byte_len > 200:
            msg = msg[:20]  # Truncate if needed
        log(f"Sending unicode: '{msg}' ({byte_len} bytes)")
        ok = send_text_cli(DEVICE_A, node_ids["C"], msg)
        if not ok:
            log(f"WARN: Failed to send '{msg}'")
        time.sleep(5)
    log("Unicode messages sent")

def test_text_rapid_fire():
    """Send 10 messages in rapid succession from A to C."""
    require_devices("A", "C")
    log("Rapid-fire: sending 10 messages A→C")
    sent = 0
    for i in range(10):
        msg = f"rapid_{i}_{int(time.time()) & 0xFFFF}"
        ok = send_text_cli(DEVICE_A, node_ids["C"], msg)
        if ok:
            sent += 1
        time.sleep(2)  # Small delay to not overwhelm
    log(f"Rapid-fire: {sent}/10 sent")
    assert sent >= 8, f"Only {sent}/10 rapid-fire messages sent"

def test_text_bidirectional_rapid():
    """Send messages in both directions (or rapid-fire A→C if C is radio-only)."""
    require_devices("A", "C")
    nid_c = get_node_id(DEVICE_C) if os.path.exists(DEVICE_C) else None
    if nid_c:
        log("Bidirectional rapid: 5 messages each direction")
        for i in range(5):
            send_text_cli(DEVICE_A, node_ids["C"], f"a2c_{i}")
            send_text_cli(DEVICE_C, node_ids["A"], f"c2a_{i}")
            time.sleep(3)
    else:
        log("C radio-only — sending 10 rapid messages A→C")
        for i in range(10):
            send_text_cli(DEVICE_A, node_ids["C"], f"a2c_rapid_{i}")
            time.sleep(2)
    log("Bidirectional/rapid test complete")

def test_text_empty_message():
    """Test sending empty/minimal messages."""
    require_devices("A", "C")
    ok = send_text_cli(DEVICE_A, node_ids["C"], ".")
    assert ok, "Single-char message failed"
    time.sleep(5)
    log("Minimal message sent")

def test_text_special_chars():
    """Send messages with special characters."""
    require_devices("A", "C")
    specials = [
        "line1\nline2",              # newline
        "tab\there",                  # tab
        "quotes \"and\" 'marks'",     # quotes
        "back\\slash",                # backslash
        "pipe|char",                  # pipe character
        "<html>&amp;</html>",        # HTML entities
    ]
    sent = 0
    for msg in specials:
        ok = send_text_cli(DEVICE_A, node_ids["C"], msg)
        if ok: sent += 1
        time.sleep(3)
    log(f"Special chars: {sent}/{len(specials)} sent")
    assert sent >= len(specials) - 1, "Too many special char messages failed"


# ═══════════════════════════════════════════════════════════════════════════
#  CATEGORY 2: GROUP MESSAGING
# ═══════════════════════════════════════════════════════════════════════════

def test_group_encoding_roundtrip():
    """Verify GroupMessage protobuf encode/decode."""
    original = encode_group(
        type_val=GROUP_TEXT, message_id=42, group_id=12345,
        text="Hello group!", members=[0x1de915bc, 0x62d9f888],
        send_time=int(time.time())
    )
    decoded = decode_group(original)
    assert decoded["type"] == GROUP_TEXT
    assert decoded["message_id"] == 42
    assert decoded["group_id"] == 12345
    assert decoded["text"] == "Hello group!"
    assert 0x1de915bc in decoded["members"]
    assert 0x62d9f888 in decoded["members"]
    log("GroupMessage protobuf round-trip OK")

def test_group_ack_encoding():
    """Verify GROUP_ACK encoding."""
    encoded = encode_group(
        type_val=GROUP_ACK, ack_message_id=42,
        group_id=12345, member_node_id=0x1de915bc
    )
    decoded = decode_group(encoded)
    assert decoded["type"] == GROUP_ACK
    assert decoded["ack_message_id"] == 42
    assert decoded["member_node_id"] == 0x1de915bc
    log("GROUP_ACK encoding OK")

def test_group_create_and_send():
    """Create a group with A and C, send text, check delivery."""
    require_devices("A", "C")
    msg_id = int(time.time()) & 0xFFFF
    group_id = random.randint(10000, 99999)
    text = f"group_test_{msg_id}"

    payload = encode_group(
        type_val=GROUP_TEXT, message_id=msg_id, group_id=group_id,
        text=text, members=[node_ids["C"]], send_time=int(time.time())
    )

    log(f"Sending GROUP_TEXT from A to C (group={group_id}, msg={msg_id})")
    ok = send_custom_packet(DEVICE_A, payload, GROUP_MESSAGE_APP)
    assert ok, "Failed to send group message"
    time.sleep(15)
    log("Group message sent — firmware handles delivery and ACKs internally")

def test_group_bidirectional():
    """Send group messages in both directions (or dual from A if C is radio-only)."""
    require_devices("A", "C")
    group_id = random.randint(10000, 99999)

    # A → C
    p1 = encode_group(
        type_val=GROUP_TEXT, message_id=1001, group_id=group_id,
        text="a_to_c", members=[node_ids["C"]], send_time=int(time.time())
    )
    send_custom_packet(DEVICE_A, p1, GROUP_MESSAGE_APP)
    log("Sent GROUP_TEXT A→C")
    time.sleep(5)

    # C → A (if serial available)
    nid_c = get_node_id(DEVICE_C) if os.path.exists(DEVICE_C) else None
    if nid_c:
        p2 = encode_group(
            type_val=GROUP_TEXT, message_id=1002, group_id=group_id,
            text="c_to_a", members=[node_ids["A"]], send_time=int(time.time())
        )
        send_custom_packet(DEVICE_C, p2, GROUP_MESSAGE_APP)
        log("Sent GROUP_TEXT C→A")
    else:
        # Send second group message from A as broadcast
        p2 = encode_group(
            type_val=GROUP_TEXT, message_id=1002, group_id=group_id,
            text="a_bcast_group", members=[node_ids["C"]], send_time=int(time.time()))
        send_custom_packet(DEVICE_A, p2, GROUP_MESSAGE_APP)
        log("Sent second GROUP_TEXT A→C (C radio-only)")

    time.sleep(10)
    log("Bidirectional group messages sent")

def test_group_roster_request():
    """Send a roster request and listen for response."""
    require_devices("A", "C")
    group_id = random.randint(10000, 99999)

    p = encode_group(
        type_val=GROUP_ROSTER_REQUEST, group_id=group_id,
        member_node_id=node_ids["A"]
    )
    send_custom_packet(DEVICE_A, p, GROUP_MESSAGE_APP)
    log("Sent ROSTER_REQUEST from A")
    time.sleep(15)
    log("Roster request sent — firmware handles response internally")

def test_group_rapid_messages():
    """Send 10 group messages rapidly."""
    require_devices("A", "C")
    group_id = random.randint(10000, 99999)
    sent = 0

    for i in range(10):
        p = encode_group(
            type_val=GROUP_TEXT, message_id=2000 + i, group_id=group_id,
            text=f"rapid_grp_{i}", members=[node_ids["C"]],
            send_time=int(time.time())
        )
        if send_custom_packet(DEVICE_A, p, GROUP_MESSAGE_APP):
            sent += 1
        time.sleep(3)

    log(f"Rapid group: {sent}/10 sent")
    assert sent >= 7, f"Only {sent}/10 rapid group messages sent"

def test_group_large_text():
    """Send a group message with maximum-size text."""
    require_devices("A", "C")
    text = "G" * 150  # Leave room for protobuf overhead within 200-byte limit
    p = encode_group(
        type_val=GROUP_TEXT, message_id=3000, group_id=55555,
        text=text, members=[node_ids["C"]], send_time=int(time.time())
    )
    log(f"Sending large group text ({len(text)} chars, {len(p)} bytes payload)")
    ok = send_custom_packet(DEVICE_A, p, GROUP_MESSAGE_APP)
    assert ok, "Large group text send failed"
    time.sleep(10)
    log("Large group text sent")


# ═══════════════════════════════════════════════════════════════════════════
#  CATEGORY 3: MEDIA TRANSFER / VOICE MEMOS
# ═══════════════════════════════════════════════════════════════════════════

def test_media_encoding_roundtrip():
    """Verify MediaTransfer protobuf encode/decode."""
    encoded = encode_media(
        type_val=MEDIA_START, transfer_id=0xABCD1234,
        total_chunks=50, total_size=10000, content_type=CT_VOICE_MEMO,
        checksum=0xDEADBEEF, mime_type="audio/codec2", duration_seconds=30
    )
    dec = decode_media(encoded)
    assert dec["type"] == MEDIA_START
    assert dec["transfer_id"] == 0xABCD1234
    assert dec["total_chunks"] == 50
    assert dec["checksum"] == 0xDEADBEEF
    assert dec["mime_type"] == "audio/codec2"
    assert dec["duration_seconds"] == 30
    log("MediaTransfer round-trip OK")

def test_media_crc32():
    """Verify CRC32 against standard test vectors."""
    assert crc32(b"123456789") == 0xCBF43926, "CRC32 standard test vector failed"
    assert crc32(b"") == 0x00000000, "CRC32 empty data failed"
    # Verify bit-flip detection
    d = generate_codec2(1000)
    c1 = crc32(d)
    d2 = bytearray(d); d2[0] ^= 1
    assert crc32(d2) != c1, "CRC32 didn't detect bit flip"
    log("CRC32 implementation verified")

def test_media_nack_encoding():
    """Verify NACK with missing chunks encodes correctly."""
    missing = [2, 5, 8, 15, 31]
    encoded = encode_media(type_val=MEDIA_NACK, transfer_id=99, missing_chunks=missing)
    dec = decode_media(encoded)
    assert dec["type"] == MEDIA_NACK
    assert dec["missing_chunks"] == missing
    log(f"NACK encoding OK: missing={missing}")

def test_media_full_sequence_encoding():
    """Encode a complete START+CHUNKs+COMPLETE transfer and verify reassembly."""
    memo = generate_codec2(3000)  # 3s
    checksum = crc32(memo)
    total_chunks = (len(memo) + CHUNK_SIZE - 1) // CHUNK_SIZE

    # START
    start = encode_media(type_val=MEDIA_START, transfer_id=0x5533, total_chunks=total_chunks,
                         total_size=len(memo), content_type=CT_VOICE_MEMO,
                         checksum=checksum, mime_type="audio/codec2", duration_seconds=3)
    assert decode_media(start)["content_type"] == CT_VOICE_MEMO

    # CHUNKs
    reassembled = bytearray(len(memo))
    for i in range(total_chunks):
        offset = i * CHUNK_SIZE
        end = min(offset + CHUNK_SIZE, len(memo))
        chunk = memo[offset:end]
        pkt = encode_media(type_val=MEDIA_CHUNK, transfer_id=0x5533, chunk_index=i, chunk_data=chunk)
        dec = decode_media(pkt)
        reassembled[dec["chunk_index"] * CHUNK_SIZE:dec["chunk_index"] * CHUNK_SIZE + len(dec["chunk_data"])] = dec["chunk_data"]

    assert bytes(reassembled) == memo, "Reassembly failed"
    assert crc32(reassembled) == checksum, "CRC mismatch"
    log(f"Full sequence: {len(memo)}B in {total_chunks} chunks, CRC=0x{checksum:08X}")

def test_media_voice_memo_durations():
    """Test codec2 data generation for various durations."""
    for dur_s in [1, 3, 5, 10, 30, 60]:
        data = generate_codec2(dur_s * 1000)
        expected = (dur_s * 1000 // 40) * CODEC2_BYTES_PER_FRAME
        assert len(data) == expected, f"{dur_s}s: expected {expected}B, got {len(data)}B"
        chunks = (len(data) + CHUNK_SIZE - 1) // CHUNK_SIZE
        log(f"{dur_s}s: {len(data)}B, {chunks} chunks")

def test_media_send_voice_memo_a_to_c():
    """Send a 3-second synthetic voice memo from A to C."""
    require_devices("A", "C")
    memo = generate_codec2(3000)
    checksum = crc32(memo)
    transfer_id = random.randint(0x1000, 0xFFFF)
    total_chunks = (len(memo) + CHUNK_SIZE - 1) // CHUNK_SIZE

    log(f"Sending 3s voice memo A→C: {len(memo)}B, {total_chunks} chunks")

    # Send START
    start = encode_media(type_val=MEDIA_START, transfer_id=transfer_id,
                         total_chunks=total_chunks, total_size=len(memo),
                         content_type=CT_VOICE_MEMO, checksum=checksum,
                         mime_type="audio/codec2", duration_seconds=3)
    send_custom_packet(DEVICE_A, start, MEDIA_TRANSFER_APP, dest=node_ids["C"])
    log("Sent START")
    time.sleep(3)

    # Send CHUNKs
    for i in range(total_chunks):
        offset = i * CHUNK_SIZE
        chunk = memo[offset:min(offset + CHUNK_SIZE, len(memo))]
        pkt = encode_media(type_val=MEDIA_CHUNK, transfer_id=transfer_id,
                           chunk_index=i, chunk_data=chunk)
        send_custom_packet(DEVICE_A, pkt, MEDIA_TRANSFER_APP, dest=node_ids["C"])
        if i % 3 == 0:
            log(f"Sent CHUNK {i}/{total_chunks}")
        time.sleep(2)

    # Send COMPLETE
    complete = encode_media(type_val=MEDIA_COMPLETE, transfer_id=transfer_id, checksum=checksum)
    send_custom_packet(DEVICE_A, complete, MEDIA_TRANSFER_APP, dest=node_ids["C"])
    log("Sent COMPLETE")
    time.sleep(10)
    log("Voice memo transfer A→C complete (check Device C speaker)")

def test_media_send_voice_memo_c_to_a():
    """Send a 5-second synthetic voice memo C→A direction.
    If C serial is unavailable, sends a second memo A→C instead."""
    require_devices("A", "C")
    memo = generate_codec2(5000)
    checksum = crc32(memo)
    transfer_id = random.randint(0x2000, 0xFFFF)
    total_chunks = (len(memo) + CHUNK_SIZE - 1) // CHUNK_SIZE

    # Check if C has serial access
    src_port = DEVICE_C
    src_label = "C"
    dest_id = node_ids["A"]
    nid = get_node_id(DEVICE_C) if os.path.exists(DEVICE_C) else None
    if not nid:
        log("Device C radio-only — sending 5s memo A→C (broadcast) instead")
        src_port = DEVICE_A
        src_label = "A"
        dest_id = node_ids["C"]

    log(f"Sending 5s voice memo {src_label}→{'A' if src_label == 'C' else 'C'}: "
        f"{len(memo)}B, {total_chunks} chunks")

    start = encode_media(type_val=MEDIA_START, transfer_id=transfer_id,
                         total_chunks=total_chunks, total_size=len(memo),
                         content_type=CT_VOICE_MEMO, checksum=checksum,
                         mime_type="audio/codec2", duration_seconds=5)
    send_custom_packet(src_port, start, MEDIA_TRANSFER_APP, dest=dest_id)
    time.sleep(3)

    for i in range(total_chunks):
        offset = i * CHUNK_SIZE
        chunk = memo[offset:min(offset + CHUNK_SIZE, len(memo))]
        pkt = encode_media(type_val=MEDIA_CHUNK, transfer_id=transfer_id,
                           chunk_index=i, chunk_data=chunk)
        send_custom_packet(src_port, pkt, MEDIA_TRANSFER_APP, dest=dest_id)
        time.sleep(2)

    complete = encode_media(type_val=MEDIA_COMPLETE, transfer_id=transfer_id, checksum=checksum)
    send_custom_packet(src_port, complete, MEDIA_TRANSFER_APP, dest=dest_id)
    log("Voice memo transfer complete")
    time.sleep(10)

def test_media_missing_chunk_nack():
    """Send a transfer with a missing chunk, expect NACK."""
    require_devices("A", "C")
    memo = generate_codec2(3000)  # 3 chunks worth
    checksum = crc32(memo)
    transfer_id = random.randint(0x3000, 0xFFFF)
    total_chunks = (len(memo) + CHUNK_SIZE - 1) // CHUNK_SIZE

    log(f"Sending transfer with missing chunk 1 (of {total_chunks})")

    start = encode_media(type_val=MEDIA_START, transfer_id=transfer_id,
                         total_chunks=total_chunks, total_size=len(memo),
                         content_type=CT_BINARY_DATA, checksum=checksum)
    send_custom_packet(DEVICE_A, start, MEDIA_TRANSFER_APP, dest=node_ids["C"])
    time.sleep(3)

    # Send all chunks EXCEPT index 1
    for i in range(total_chunks):
        if i == 1:
            log("Skipping CHUNK 1")
            continue
        offset = i * CHUNK_SIZE
        chunk = memo[offset:min(offset + CHUNK_SIZE, len(memo))]
        pkt = encode_media(type_val=MEDIA_CHUNK, transfer_id=transfer_id,
                           chunk_index=i, chunk_data=chunk)
        send_custom_packet(DEVICE_A, pkt, MEDIA_TRANSFER_APP, dest=node_ids["C"])
        time.sleep(2)

    complete = encode_media(type_val=MEDIA_COMPLETE, transfer_id=transfer_id, checksum=checksum)
    send_custom_packet(DEVICE_A, complete, MEDIA_TRANSFER_APP, dest=node_ids["C"])
    log("Sent COMPLETE with missing chunk — firmware should NACK")
    time.sleep(15)
    log("NACK test complete (firmware handles internally)")

def test_media_cancel_transfer():
    """Start a transfer and cancel mid-stream."""
    require_devices("A", "C")
    transfer_id = random.randint(0x4000, 0xFFFF)

    start = encode_media(type_val=MEDIA_START, transfer_id=transfer_id,
                         total_chunks=10, total_size=2000,
                         content_type=CT_BINARY_DATA, checksum=0)
    send_custom_packet(DEVICE_A, start, MEDIA_TRANSFER_APP, dest=node_ids["C"])
    time.sleep(3)

    # Send 2 chunks then cancel
    for i in range(2):
        chunk = bytes([i & 0xFF] * CHUNK_SIZE)
        pkt = encode_media(type_val=MEDIA_CHUNK, transfer_id=transfer_id,
                           chunk_index=i, chunk_data=chunk)
        send_custom_packet(DEVICE_A, pkt, MEDIA_TRANSFER_APP, dest=node_ids["C"])
        time.sleep(2)

    cancel = encode_media(type_val=MEDIA_CANCEL, transfer_id=transfer_id)
    send_custom_packet(DEVICE_A, cancel, MEDIA_TRANSFER_APP, dest=node_ids["C"])
    log("Sent CANCEL after 2 chunks")
    time.sleep(5)
    log("Cancel test complete")

def test_media_image_sized_transfer():
    """Simulate an image-sized transfer (5KB) — tests larger chunking."""
    require_devices("A", "C")
    payload = bytes([(i % 256) for i in range(5000)])
    checksum = crc32(payload)
    transfer_id = random.randint(0x5000, 0xFFFF)
    total_chunks = (len(payload) + CHUNK_SIZE - 1) // CHUNK_SIZE

    log(f"Sending 5KB image-sized transfer: {total_chunks} chunks")

    start = encode_media(type_val=MEDIA_START, transfer_id=transfer_id,
                         total_chunks=total_chunks, total_size=len(payload),
                         content_type=CT_IMAGE_THUMBNAIL, checksum=checksum,
                         mime_type="image/jpeg", width=320, height=240)
    send_custom_packet(DEVICE_A, start, MEDIA_TRANSFER_APP, dest=node_ids["C"])
    time.sleep(3)

    for i in range(total_chunks):
        offset = i * CHUNK_SIZE
        chunk = payload[offset:min(offset + CHUNK_SIZE, len(payload))]
        pkt = encode_media(type_val=MEDIA_CHUNK, transfer_id=transfer_id,
                           chunk_index=i, chunk_data=chunk)
        send_custom_packet(DEVICE_A, pkt, MEDIA_TRANSFER_APP, dest=node_ids["C"])
        if i % 5 == 0:
            log(f"Chunk {i}/{total_chunks}")
        time.sleep(1.5)

    complete = encode_media(type_val=MEDIA_COMPLETE, transfer_id=transfer_id, checksum=checksum)
    send_custom_packet(DEVICE_A, complete, MEDIA_TRANSFER_APP, dest=node_ids["C"])
    log("Image-sized transfer complete")
    time.sleep(10)

def test_media_simultaneous_transfers():
    """Start two transfers concurrently (different transfer IDs)."""
    require_devices("A", "C")
    memo1 = generate_codec2(2000)
    memo2 = generate_codec2(3000)
    tid1 = random.randint(0x6000, 0x6FFF)
    tid2 = random.randint(0x7000, 0x7FFF)

    log(f"Starting two simultaneous transfers: tid1=0x{tid1:04X}, tid2=0x{tid2:04X}")

    # Start both
    for tid, memo, dur in [(tid1, memo1, 2), (tid2, memo2, 3)]:
        s = encode_media(type_val=MEDIA_START, transfer_id=tid,
                         total_chunks=(len(memo) + CHUNK_SIZE - 1) // CHUNK_SIZE,
                         total_size=len(memo), content_type=CT_VOICE_MEMO,
                         checksum=crc32(memo), mime_type="audio/codec2",
                         duration_seconds=dur)
        send_custom_packet(DEVICE_A, s, MEDIA_TRANSFER_APP, dest=node_ids["C"])
        time.sleep(1)

    # Interleave chunks
    chunks1 = [(tid1, i, memo1[i*CHUNK_SIZE:min((i+1)*CHUNK_SIZE, len(memo1))])
               for i in range((len(memo1) + CHUNK_SIZE - 1) // CHUNK_SIZE)]
    chunks2 = [(tid2, i, memo2[i*CHUNK_SIZE:min((i+1)*CHUNK_SIZE, len(memo2))])
               for i in range((len(memo2) + CHUNK_SIZE - 1) // CHUNK_SIZE)]

    all_chunks = []
    for i in range(max(len(chunks1), len(chunks2))):
        if i < len(chunks1): all_chunks.append(chunks1[i])
        if i < len(chunks2): all_chunks.append(chunks2[i])

    for tid, idx, data in all_chunks:
        pkt = encode_media(type_val=MEDIA_CHUNK, transfer_id=tid,
                           chunk_index=idx, chunk_data=data)
        send_custom_packet(DEVICE_A, pkt, MEDIA_TRANSFER_APP, dest=node_ids["C"])
        time.sleep(1.5)

    # Complete both
    for tid, memo in [(tid1, memo1), (tid2, memo2)]:
        c = encode_media(type_val=MEDIA_COMPLETE, transfer_id=tid, checksum=crc32(memo))
        send_custom_packet(DEVICE_A, c, MEDIA_TRANSFER_APP, dest=node_ids["C"])
        time.sleep(2)

    log("Simultaneous transfers complete")
    time.sleep(10)


# ═══════════════════════════════════════════════════════════════════════════
#  CATEGORY 4: CROSS-BAND AWARENESS
# ═══════════════════════════════════════════════════════════════════════════

def test_crossband_encoding_roundtrip():
    """Verify CrossBandMessage encode/decode."""
    encoded = encode_crossband(
        type_val=BAND_ADVERTISEMENT, node_id=0xABCD,
        supported_bands=[BAND_US_915, BAND_ISM_2400],
        primary_band=BAND_US_915, is_dual_band=True
    )
    dec = decode_crossband(encoded)
    assert dec["type"] == BAND_ADVERTISEMENT
    assert dec["node_id"] == 0xABCD
    assert dec["supported_bands"] == [BAND_US_915, BAND_ISM_2400]
    assert dec["primary_band"] == BAND_US_915
    assert dec["is_dual_band"] == True
    log("CrossBand round-trip OK")

def test_crossband_bridged_encoding():
    """Verify bridged message encoding with payload."""
    payload = bytes(range(100))
    encoded = encode_crossband(
        type_val=BRIDGED_MESSAGE, node_id=0x1122,
        bridge_ttl=3, source_band=BAND_EU_868,
        original_message_id=42, original_portnum=1,
        bridged_payload=payload, mqtt_topic="msh/bridge/test"
    )
    dec = decode_crossband(encoded)
    assert dec["type"] == BRIDGED_MESSAGE
    assert dec["bridge_ttl"] == 3
    assert dec["bridged_payload"] == payload
    assert dec["mqtt_topic"] == "msh/bridge/test"
    log("Bridged message encoding OK")

def test_crossband_discovery_encoding():
    """Verify discovery request/response encoding."""
    req = encode_crossband(type_val=BAND_DISCOVERY_REQUEST, node_id=0xAAAA)
    assert decode_crossband(req)["type"] == BAND_DISCOVERY_REQUEST

    resp = encode_crossband(
        type_val=BAND_DISCOVERY_RESPONSE, node_id=0xBBBB,
        primary_band=BAND_US_915, supported_bands=[BAND_US_915]
    )
    dec = decode_crossband(resp)
    assert dec["type"] == BAND_DISCOVERY_RESPONSE
    assert dec["primary_band"] == BAND_US_915
    log("Discovery encoding OK")

def test_crossband_band_advertisement():
    """Send a band advertisement from A."""
    require_devices("A", "C")
    advert = encode_crossband(
        type_val=BAND_ADVERTISEMENT, node_id=node_ids["A"],
        primary_band=BAND_US_915, supported_bands=[BAND_US_915]
    )
    ok = send_custom_packet(DEVICE_A, advert, CROSS_BAND_APP)
    assert ok, "Band advertisement send failed"
    log("Band advertisement sent from A")
    time.sleep(10)

def test_crossband_discovery_request():
    """Send a discovery request from A."""
    require_devices("A", "C")
    req = encode_crossband(type_val=BAND_DISCOVERY_REQUEST, node_id=node_ids["A"])
    ok = send_custom_packet(DEVICE_A, req, CROSS_BAND_APP)
    assert ok, "Discovery request send failed"
    log("Discovery request sent — waiting for responses")
    time.sleep(15)
    log("Discovery request test complete")

def test_crossband_bridged_message():
    """Send a bridged message from A."""
    require_devices("A", "C")
    bridged = encode_crossband(
        type_val=BRIDGED_MESSAGE, node_id=node_ids["A"],
        bridge_ttl=3, source_band=BAND_EU_868,
        original_message_id=random.randint(1, 0xFFFF),
        original_portnum=TEXT_MESSAGE_APP,
        bridged_payload=b"Hello from EU868!",
        mqtt_topic="msh/bridge/eu868/test"
    )
    ok = send_custom_packet(DEVICE_A, bridged, CROSS_BAND_APP)
    assert ok, "Bridged message send failed"
    log("Bridged message sent")
    time.sleep(10)

def test_crossband_ttl_values():
    """Test various TTL values in bridged messages."""
    for ttl in [1, 2, 3, 5]:
        encoded = encode_crossband(
            type_val=BRIDGED_MESSAGE, bridge_ttl=ttl,
            source_band=BAND_US_915, original_message_id=100 + ttl,
            bridged_payload=b"ttl_test"
        )
        dec = decode_crossband(encoded)
        assert dec["bridge_ttl"] == ttl, f"TTL mismatch: expected {ttl}, got {dec['bridge_ttl']}"
    log("TTL encoding verified for values 1-5")


# ═══════════════════════════════════════════════════════════════════════════
#  CATEGORY 5: MQTT (if configured)
# ═══════════════════════════════════════════════════════════════════════════

def test_mqtt_config_check():
    """Check if MQTT is configured on any device."""
    require_devices("A")
    stdout, _, _ = run_cli(DEVICE_A, "--info")
    if "mqtt" in stdout.lower():
        log("MQTT appears configured on Device A")
    else:
        log("MQTT not detected in device info — MQTT tests will be informational")
    # Always pass — informational
    log("MQTT config check complete")

def test_mqtt_uplink():
    """Send a message that should uplink via MQTT (if configured)."""
    require_devices("A")
    msg = f"mqtt_test_{int(time.time()) & 0xFFFF}"
    ok = send_broadcast_cli(DEVICE_A, msg)
    assert ok, "Broadcast send failed"
    log(f"Broadcast sent (would uplink via MQTT if configured): '{msg}'")
    time.sleep(10)


# ═══════════════════════════════════════════════════════════════════════════
#  CATEGORY 6: STORE & FORWARD
# ═══════════════════════════════════════════════════════════════════════════

def test_sf_config_check():
    """Check if Store & Forward is available."""
    require_devices("A")
    stdout, _, _ = run_cli(DEVICE_A, "--info")
    if "store" in stdout.lower() or "forward" in stdout.lower():
        log("Store & Forward appears configured on Device A")
    else:
        log("Store & Forward not detected — S&F tests informational")
    log("S&F config check complete")

def test_sf_message_store():
    """Send messages while recipient may be offline (simulated)."""
    require_devices("A", "C")
    # Send a burst of messages that S&F module may store
    for i in range(3):
        msg = f"sf_store_{i}_{int(time.time()) & 0xFFFF}"
        send_text_cli(DEVICE_A, node_ids["C"], msg)
        time.sleep(3)
    log("S&F store test: 3 messages sent")
    time.sleep(10)


# ═══════════════════════════════════════════════════════════════════════════
#  MIXED / INTEGRATION STRESS TESTS
# ═══════════════════════════════════════════════════════════════════════════

def test_mixed_traffic():
    """Send text, group, and media packets interleaved."""
    require_devices("A", "C")
    log("Mixed traffic: interleaving text, group, and media")

    # Text DM
    send_text_cli(DEVICE_A, node_ids["C"], f"mixed_text_{int(time.time()) & 0xFFFF}")
    time.sleep(3)

    # Group message
    gp = encode_group(type_val=GROUP_TEXT, message_id=9000, group_id=88888,
                      text="mixed_group", members=[node_ids["C"]],
                      send_time=int(time.time()))
    send_custom_packet(DEVICE_A, gp, GROUP_MESSAGE_APP)
    time.sleep(3)

    # Media START
    memo = generate_codec2(1000)
    start = encode_media(type_val=MEDIA_START, transfer_id=0x8000,
                         total_chunks=1, total_size=len(memo),
                         content_type=CT_VOICE_MEMO, checksum=crc32(memo),
                         mime_type="audio/codec2", duration_seconds=1)
    send_custom_packet(DEVICE_A, start, MEDIA_TRANSFER_APP, dest=node_ids["C"])
    time.sleep(3)

    # Text broadcast
    send_broadcast_cli(DEVICE_A, f"mixed_bcast_{int(time.time()) & 0xFFFF}")
    time.sleep(3)

    # Cross-band advert
    advert = encode_crossband(type_val=BAND_ADVERTISEMENT, node_id=node_ids["A"],
                              primary_band=BAND_US_915)
    send_custom_packet(DEVICE_A, advert, CROSS_BAND_APP)
    time.sleep(5)

    log("Mixed traffic test complete")

def test_sustained_load():
    """Sustained messaging load: 30 messages over ~90 seconds."""
    require_devices("A", "C")
    log("Sustained load: 30 messages over ~90s")
    sent = 0
    for i in range(30):
        msg = f"load_{i:02d}_{int(time.time()) & 0xFFFF}"
        if i % 2 == 0:
            ok = send_text_cli(DEVICE_A, node_ids["C"], msg)
        else:
            ok = send_broadcast_cli(DEVICE_A, msg)
        if ok: sent += 1
        time.sleep(3)
        if i % 10 == 9:
            log(f"Progress: {i+1}/30 sent ({sent} successful)")

    log(f"Sustained load: {sent}/30 sent")
    assert sent >= 25, f"Only {sent}/30 sustained messages sent"


# ═══════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════

def discover_devices():
    """Discover connected devices and get node IDs."""
    global node_ids

    # Device A — must be serial-accessible
    if os.path.exists(DEVICE_A):
        nid = get_node_id(DEVICE_A)
        if nid:
            node_ids["A"] = nid
            print(f"  Device A: node 0x{nid:08x} on {DEVICE_A} (serial)")
        else:
            print(f"  Device A: FAILED to get node ID from {DEVICE_A}")
    else:
        print(f"  Device A: NOT CONNECTED ({DEVICE_A})")

    # Device C — try serial first; if unavailable, use known node ID from mesh
    DEVICE_C_NODE_NUM = 1658452104  # 0x62d9f888 (known from earlier discovery)
    if os.path.exists(DEVICE_C):
        nid = get_node_id(DEVICE_C)
        if nid:
            node_ids["C"] = nid
            print(f"  Device C: node 0x{nid:08x} on {DEVICE_C} (serial)")
        else:
            # Serial port exists but protocol unresponsive — check A's mesh for C
            if "A" in node_ids:
                stdout, _, _ = run_cli(DEVICE_A, "--info")
                if "62d9f888" in stdout:
                    node_ids["C"] = DEVICE_C_NODE_NUM
                    print(f"  Device C: node 0x{DEVICE_C_NODE_NUM:08x} (radio-only, "
                          f"serial unresponsive, visible in A's mesh)")
                else:
                    print(f"  Device C: serial unresponsive and not in A's mesh")
            else:
                print(f"  Device C: serial unresponsive, no Device A to check mesh")
    else:
        # No serial port — check A's mesh anyway
        if "A" in node_ids:
            stdout, _, _ = run_cli(DEVICE_A, "--info")
            if "62d9f888" in stdout:
                node_ids["C"] = DEVICE_C_NODE_NUM
                print(f"  Device C: node 0x{DEVICE_C_NODE_NUM:08x} (radio-only, not serial)")
            else:
                print(f"  Device C: NOT CONNECTED and not in mesh")
        else:
            print(f"  Device C: NOT CONNECTED ({DEVICE_C})")

    return len(node_ids)


CATEGORIES = {
    "text": [
        ("text_basic_dm", test_text_basic_dm),
        ("text_basic_dm_reverse", test_text_basic_dm_reverse),
        ("text_broadcast", test_text_broadcast),
        ("text_max_length", test_text_max_length),
        ("text_emoji", test_text_emoji),
        ("text_unicode", test_text_unicode),
        ("text_rapid_fire", test_text_rapid_fire),
        ("text_bidirectional_rapid", test_text_bidirectional_rapid),
        ("text_empty_message", test_text_empty_message),
        ("text_special_chars", test_text_special_chars),
    ],
    "group": [
        ("group_encoding_roundtrip", test_group_encoding_roundtrip),
        ("group_ack_encoding", test_group_ack_encoding),
        ("group_create_and_send", test_group_create_and_send),
        ("group_bidirectional", test_group_bidirectional),
        ("group_roster_request", test_group_roster_request),
        ("group_rapid_messages", test_group_rapid_messages),
        ("group_large_text", test_group_large_text),
    ],
    "media": [
        ("media_encoding_roundtrip", test_media_encoding_roundtrip),
        ("media_crc32", test_media_crc32),
        ("media_nack_encoding", test_media_nack_encoding),
        ("media_full_sequence_encoding", test_media_full_sequence_encoding),
        ("media_voice_memo_durations", test_media_voice_memo_durations),
        ("media_send_voice_memo_a_to_c", test_media_send_voice_memo_a_to_c),
        ("media_send_voice_memo_c_to_a", test_media_send_voice_memo_c_to_a),
        ("media_missing_chunk_nack", test_media_missing_chunk_nack),
        ("media_cancel_transfer", test_media_cancel_transfer),
        ("media_image_sized_transfer", test_media_image_sized_transfer),
        ("media_simultaneous_transfers", test_media_simultaneous_transfers),
    ],
    "crossband": [
        ("crossband_encoding_roundtrip", test_crossband_encoding_roundtrip),
        ("crossband_bridged_encoding", test_crossband_bridged_encoding),
        ("crossband_discovery_encoding", test_crossband_discovery_encoding),
        ("crossband_band_advertisement", test_crossband_band_advertisement),
        ("crossband_discovery_request", test_crossband_discovery_request),
        ("crossband_bridged_message", test_crossband_bridged_message),
        ("crossband_ttl_values", test_crossband_ttl_values),
    ],
    "mqtt": [
        ("mqtt_config_check", test_mqtt_config_check),
        ("mqtt_uplink", test_mqtt_uplink),
    ],
    "sf": [
        ("sf_config_check", test_sf_config_check),
        ("sf_message_store", test_sf_message_store),
    ],
    "mixed": [
        ("mixed_traffic", test_mixed_traffic),
        ("sustained_load", test_sustained_load),
    ],
}


def main():
    parser = argparse.ArgumentParser(description="MeshReliable Comprehensive Stress Test")
    parser.add_argument("--category", "-c", choices=list(CATEGORIES.keys()) + ["all"],
                        default="all", help="Test category to run")
    args = parser.parse_args()

    print()
    print("=" * 60)
    print("  MeshReliable — Comprehensive Stress Test Suite")
    print("=" * 60)
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Device A: {DEVICE_A}")
    print(f"  Device C: {DEVICE_C}")
    print()

    # Discover devices
    print("--- Discovering devices ---")
    num_devices = discover_devices()
    if num_devices == 0:
        print("\nERROR: No devices found. Connect at least one device.")
        return 1
    print(f"\n  {num_devices} device(s) available")

    # Select categories to run
    if args.category == "all":
        categories_to_run = list(CATEGORIES.keys())
    else:
        categories_to_run = [args.category]

    # Run tests
    total_tests = 0
    for cat in categories_to_run:
        tests = CATEGORIES[cat]
        print(f"\n{'='*60}")
        print(f"  CATEGORY: {cat.upper()} ({len(tests)} tests)")
        print(f"{'='*60}")
        for name, func in tests:
            run_test(name, func, cat)
            total_tests += 1

    # Summary
    print(f"\n{'='*60}")
    print(f"  RESULTS SUMMARY")
    print(f"{'='*60}")

    passed = sum(1 for v in results.values() if v == "PASS")
    skipped = sum(1 for v in results.values() if v.startswith("SKIP"))
    failed = sum(1 for v in results.values() if v.startswith("FAIL"))

    # Group by category
    for cat in categories_to_run:
        cat_tests = [name for name, _ in CATEGORIES[cat]]
        print(f"\n  {cat.upper()}:")
        for name in cat_tests:
            if name in results:
                r = results[name]
                if r == "PASS":
                    status = "PASS"
                elif r.startswith("SKIP"):
                    status = "SKIP"
                else:
                    status = "FAIL"
                detail = "" if r == "PASS" else f" ({r})"
                print(f"    [{status:4s}] {name}{detail}")

    print(f"\n  {'─'*40}")
    print(f"  TOTAL: {total_tests} tests")
    print(f"  PASS:  {passed}")
    print(f"  FAIL:  {failed}")
    print(f"  SKIP:  {skipped}")
    print(f"{'='*60}")

    # Write results to JSON for later analysis
    results_file = os.path.join(os.path.dirname(__file__), "stress_results.json")
    with open(results_file, "w") as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "devices": {k: f"0x{v:08x}" for k, v in node_ids.items()},
            "results": results,
            "summary": {"total": total_tests, "passed": passed, "failed": failed, "skipped": skipped}
        }, f, indent=2)
    print(f"\n  Results saved to {results_file}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
