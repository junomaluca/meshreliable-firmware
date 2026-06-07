#!/usr/bin/env python3
"""
MeshReliable — Four-Device 144 MHz VHF Stress Test

Tests all feature categories across four VHF T-Beam devices with full
send/receive rate tracking:

  Phase 1: Text Messages (1500+ total)
    - 200 DMs per pair (6 pairs = 1200 DMs)
    - 200 broadcasts (50 per device)
    - 100 channel messages (25 per device on ch0 "maluca")

  Phase 2: Voice Memos (portnum 259)
    - 3s, 5s, 10s memos between all pairs (20+ transfers)
    - Chunked: START -> CHUNK(s) -> COMPLETE -> wait for ACK/NACK

  Phase 3: Image Transfer (portnum 259)
    - 100-500 byte thumbnails between pairs (20+ transfers)
    - Same chunked protocol as voice memos

  Phase 4: Mixed Traffic Stress
    - Simultaneous text + media from multiple devices
    - Rapid-fire bursts (0.5s intervals)
    - Long messages (200+ chars)

Devices (all 144 MHz VHF / ITU2_2M, tbeam-s3-core):
  VHF-A: /dev/cu.usbmodem101      node !335e1be8 (861805544)
  VHF-B: /dev/cu.usbmodem1101     node !335e1bdc (861805532)
  BPF-A: /dev/cu.usbmodem21101    node !189ef084 (413069444)
  BPF-B: /dev/cu.usbmodem21201    node !67676264 (1734828644)

Usage:
    python3 tests/four_device_stress.py
    python3 tests/four_device_stress.py --phase 1
    python3 tests/four_device_stress.py --phase 2
    python3 tests/four_device_stress.py --phase 3
    python3 tests/four_device_stress.py --phase 4
    python3 tests/four_device_stress.py --quick       # reduced counts for smoke test
"""

import os
import sys
import time
import random
import struct
import subprocess
import argparse
import json
import threading
import queue
import signal
import traceback
from datetime import datetime
from collections import defaultdict

# ---------------------------------------------------------------------------
# Device configuration
# ---------------------------------------------------------------------------

MESHTASTIC = "/Users/patrick/Library/Python/3.14/bin/meshtastic"

DEVICES = {
    "VHF-A": {
        "port":    "/dev/cu.usbmodem101",
        "node_id": 861805544,       # !335e1be8
        "node_hex": "335e1be8",
    },
    "VHF-B": {
        "port":    "/dev/cu.usbmodem1101",
        "node_id": 861805532,       # !335e1bdc
        "node_hex": "335e1bdc",
    },
    "BPF-A": {
        "port":    "/dev/cu.usbmodem21101",
        "node_id": 1472105209,      # !57b54af9
        "node_hex": "57b54af9",
    },
    "BPF-B": {
        "port":    "/dev/cu.usbmodem21201",
        "node_id": 932425505,       # !37900b21
        "node_hex": "37900b21",
    },
}

# All ordered pairs for DM testing (bidirectional = both directions)
ALL_PAIRS = [
    ("VHF-A", "VHF-B"),
    ("VHF-A", "BPF-A"),
    ("VHF-A", "BPF-B"),
    ("VHF-B", "BPF-A"),
    ("VHF-B", "BPF-B"),
    ("BPF-A", "BPF-B"),
]

DEVICE_LIST = list(DEVICES.keys())

# Portnum constants
TEXT_MESSAGE_APP    = 1
MEDIA_TRANSFER_APP  = 259

# MediaTransfer message types
MEDIA_CHUNK        = 0
MEDIA_START        = 1
MEDIA_COMPLETE     = 2
MEDIA_NACK         = 3
MEDIA_ACK_COMPLETE = 4
MEDIA_CANCEL       = 5

# MediaContentType
CT_VOICE_MEMO      = 0
CT_IMAGE_THUMBNAIL = 1
CT_IMAGE_LOWRES    = 2
CT_BINARY_DATA     = 3

CHUNK_SIZE = 200

# Radio timing — 144 MHz LoRa is slow; be respectful of TX queue contention
# Auto-sent packets (telemetry, position) compete for airtime, so chunks need
# generous spacing to avoid queue-ordering issues that cause NACKs.
TEXT_DELAY   = 2.0   # seconds between text sends
MEDIA_DELAY  = 10.0  # seconds between media chunk sends (increased for TX queue clearance)
BURST_DELAY  = 0.5   # for burst phase

# Receive wait windows
TEXT_WAIT    = 30    # seconds to wait for text ACK
MEDIA_WAIT   = 90    # seconds to wait for media ACK_COMPLETE/NACK (increased for VHF)

# ---------------------------------------------------------------------------
# Global results tracking
# ---------------------------------------------------------------------------

stats = defaultdict(lambda: {
    "sent": 0, "send_ok": 0, "send_fail": 0,
    "recv_expected": 0, "recv_ok": 0,
    "errors": [],
})

# per-transfer tracking for media
media_transfers = {}   # transfer_id -> {"sent_chunks": N, "acked": bool, "nacked": bool}

# per-phase summary
phase_results = {}

start_time = None


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def log(msg, level="INFO"):
    ts = time.strftime("%H:%M:%S")
    prefix = {"INFO": "  ", "WARN": "  WARN ", "ERR": "  ERR  ", "OK": "  OK   "}[level]
    print(f"[{ts}]{prefix}{msg}", flush=True)

def section(title):
    print(f"\n{'='*68}", flush=True)
    print(f"  {title}", flush=True)
    print(f"{'='*68}", flush=True)

def subsection(title):
    print(f"\n{'─'*68}", flush=True)
    print(f"  {title}", flush=True)
    print(f"{'─'*68}", flush=True)


# ---------------------------------------------------------------------------
# Protobuf encode helpers  (identical to test_stress.py)
# ---------------------------------------------------------------------------

def encode_varint(value):
    buf = bytearray()
    if value == 0:
        buf.append(0)
        return buf
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


# ---------------------------------------------------------------------------
# MediaTransfer encode/decode
# ---------------------------------------------------------------------------

def encode_media(type_val=0, transfer_id=0, chunk_index=0, total_chunks=0,
                 total_size=0, chunk_data=b"", content_type=0,
                 missing_chunks=None, checksum=0, mime_type="",
                 duration_seconds=0, width=0, height=0):
    d = bytearray()
    d.extend(encode_fv(1, type_val))
    d.extend(encode_fv(2, transfer_id))
    d.extend(encode_fv(3, chunk_index))
    d.extend(encode_fv(4, total_chunks))
    d.extend(encode_fv(5, total_size))
    d.extend(encode_fb(6, chunk_data))
    d.extend(encode_fv(7, content_type))
    d.extend(encode_fp(8, missing_chunks or []))
    d.extend(encode_fv(9, checksum))
    d.extend(encode_fs(10, mime_type))
    d.extend(encode_fv(11, duration_seconds))
    d.extend(encode_fv(12, width))
    d.extend(encode_fv(13, height))
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


# ---------------------------------------------------------------------------
# CRC32  (identical to test_stress.py)
# ---------------------------------------------------------------------------

def crc32(data):
    crc = 0xFFFFFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ 0xEDB88320 if crc & 1 else crc >> 1
    return (~crc) & 0xFFFFFFFF


# ---------------------------------------------------------------------------
# Synthetic data generators
# ---------------------------------------------------------------------------

def generate_codec2(duration_ms=5000):
    """Generate deterministic synthetic Codec2 data at 4 bytes/frame, 40ms/frame."""
    total_frames = duration_ms // 40
    data = bytearray()
    for f in range(total_frames):
        data.append((f * 7  + 0x42) & 0xFF)
        data.append((f * 13 + 0xA3) & 0xFF)
        data.append((f * 31 + 0x17) & 0xFF)
        data.append((f * 53 + 0x9E) & 0xFF)
    return bytes(data)

def generate_test_image(size=256):
    """Generate deterministic test image data (simulates compressed thumbnail)."""
    # JPEG-like header magic + pseudo-random body
    header = bytes([0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, 0x4A, 0x46, 0x49, 0x46])
    body = bytes([(i * 37 + 0xAA) & 0xFF for i in range(size - len(header) - 2)])
    footer = bytes([0xFF, 0xD9])
    return header + body + footer


# ---------------------------------------------------------------------------
# Send helpers
# ---------------------------------------------------------------------------

def _with_timeout(fn, timeout_s=20):
    """Run fn() with a hard wall-clock timeout."""
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        f = pool.submit(fn)
        return f.result(timeout=timeout_s)


def send_text_cli(from_label, dest_label, message, timeout=30):
    """Send a DM via CLI. Returns True on success."""
    port    = DEVICES[from_label]["port"]
    dest_id = DEVICES[dest_label]["node_id"]
    dest_hex = f"!{dest_id:08x}"
    cmd = [MESHTASTIC, "--port", port, "--dest", dest_hex, "--sendtext", message]
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            proc.communicate(timeout=timeout)
            return proc.returncode == 0
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
            return False
    except Exception as e:
        log(f"send_text_cli error {from_label}->{dest_label}: {e}", "ERR")
        return False


def send_broadcast_cli(from_label, message, timeout=30):
    """Send a broadcast text via CLI. Returns True on success."""
    port = DEVICES[from_label]["port"]
    cmd = [MESHTASTIC, "--port", port, "--sendtext", message]
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            proc.communicate(timeout=timeout)
            return proc.returncode == 0
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
            return False
    except Exception as e:
        log(f"send_broadcast_cli error {from_label}: {e}", "ERR")
        return False


def send_channel_cli(from_label, message, channel=0, timeout=30):
    """Send a message on a specific channel index via CLI."""
    port = DEVICES[from_label]["port"]
    cmd = [MESHTASTIC, "--port", port, "--ch-index", str(channel), "--sendtext", message]
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            proc.communicate(timeout=timeout)
            return proc.returncode == 0
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
            return False
    except Exception as e:
        log(f"send_channel_cli error {from_label}: {e}", "ERR")
        return False


def send_custom_packet(from_label, payload_bytes, portnum, dest_label=None, retries=2):
    """Connect briefly via Python API, send a custom portnum packet, disconnect."""
    port = DEVICES[from_label]["port"]
    dest_id = DEVICES[dest_label]["node_id"] if dest_label else "^all"

    import meshtastic
    import meshtastic.serial_interface

    for attempt in range(retries):
        iface = None
        try:
            iface = _with_timeout(
                lambda: meshtastic.serial_interface.SerialInterface(port), 20)
            time.sleep(1.5)
            iface.sendData(
                payload_bytes,
                destinationId=dest_id,
                portNum=portnum,
                wantAck=True,
                wantResponse=False,
            )
            time.sleep(0.5)
            iface.close()
            time.sleep(1)
            return True
        except Exception as e:
            if iface:
                try: iface.close()
                except: pass
            if attempt < retries - 1:
                log(f"send_custom_packet retry {attempt+1}: {e}", "WARN")
                time.sleep(3)
            else:
                dest_str = "^all" if not dest_label else dest_label
                log(f"send_custom_packet failed ({from_label}->{dest_str}): {e}", "ERR")
                return False
    return False


def send_media_persistent(from_label, dest_label, packets_list, timeout_for_ack=60):
    """
    Send a sequence of media packets via a SINGLE persistent serial connection
    on the SOURCE device, then wait for ACK_COMPLETE/NACK on the same connection.

    packets_list: list of (payload_bytes, delay_after) tuples
    Returns: (chunks_sent, ack_response_dict_or_None)
    """
    port = DEVICES[from_label]["port"]
    dest_id = DEVICES[dest_label]["node_id"]

    import meshtastic
    import meshtastic.serial_interface
    from pubsub import pub

    ack_event = threading.Event()
    ack_result = [None]
    expected_tid = [0]

    def on_receive(packet, interface):
        decoded = packet.get("decoded", {})
        portnum_val = decoded.get("portnum")
        # meshtastic python lib returns portnum as string name or int
        if portnum_val not in ("MEDIA_TRANSFER_APP", MEDIA_TRANSFER_APP, 259):
            return
        payload = decoded.get("payload", b"")
        if not isinstance(payload, (bytes, bytearray)) or len(payload) == 0:
            return
        m = decode_media(payload)
        if (m["transfer_id"] == expected_tid[0] and
                m["type"] in (MEDIA_ACK_COMPLETE, MEDIA_NACK)):
            ack_result[0] = m
            ack_event.set()

    iface = None
    try:
        iface = _with_timeout(
            lambda: meshtastic.serial_interface.SerialInterface(port), 20)
        time.sleep(1.5)

        pub.subscribe(on_receive, "meshtastic.receive")

        chunks_sent = 0
        for i, (payload, delay) in enumerate(packets_list):
            # Extract transfer_id from first packet for ACK matching
            if i == 0:
                m = decode_media(payload)
                expected_tid[0] = m["transfer_id"]
            try:
                iface.sendData(
                    payload,
                    destinationId=dest_id,
                    portNum=MEDIA_TRANSFER_APP,
                    wantAck=True,
                    wantResponse=False,
                )
                chunks_sent += 1
            except Exception as e:
                log(f"  Packet {i} send failed: {e}", "WARN")
            if delay > 0:
                time.sleep(delay)

        # Wait for ACK_COMPLETE or NACK
        ack_event.wait(timeout=timeout_for_ack)

        try:
            pub.unsubscribe(on_receive, "meshtastic.receive")
        except Exception:
            pass
        iface.close()
        time.sleep(1)
        return chunks_sent, ack_result[0]

    except Exception as e:
        log(f"send_media_persistent error ({from_label}->{dest_label}): {e}", "ERR")
        try:
            pub.unsubscribe(on_receive, "meshtastic.receive")
        except Exception:
            pass
        if iface:
            try: iface.close()
            except: pass
        return 0, None


def get_node_id(label):
    """Confirm device is reachable and return its node ID via CLI."""
    port = DEVICES[label]["port"]
    if not os.path.exists(port):
        return None
    cmd = [MESHTASTIC, "--port", port, "--info"]
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            stdout, stderr = proc.communicate(timeout=20)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
            return None
        import re
        m = re.search(r'"myNodeNum":\s*(\d+)', stdout)
        return int(m.group(1)) if m else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Receive listener (runs in a background thread, one device at a time)
# ---------------------------------------------------------------------------

class PacketListener:
    """Opens a meshtastic serial connection and records all received packets."""

    def __init__(self, label):
        self.label = label
        self.port = DEVICES[label]["port"]
        self.received = []
        self._iface = None
        self._lock = threading.Lock()
        self._running = False

    def start(self):
        import meshtastic
        import meshtastic.serial_interface
        from pubsub import pub

        def on_receive(packet, interface):
            with self._lock:
                self.received.append({
                    "time": time.time(),
                    "packet": packet,
                })

        try:
            self._iface = _with_timeout(
                lambda: meshtastic.serial_interface.SerialInterface(self.port), 20)
            # Use unique topic per listener to avoid pubsub collision
            topic = f"meshtastic.receive.{self.label}"
            # pubsub doesn't support per-topic subscription for the mesh
            # library events — use the global topic
            pub.subscribe(on_receive, "meshtastic.receive")
            self._running = True
            log(f"Listener started on {self.label} ({self.port})")
        except Exception as e:
            log(f"Listener failed to start on {self.label}: {e}", "ERR")
            self._running = False

    def stop(self):
        from pubsub import pub
        try:
            pub.unsubAll()
        except Exception:
            pass
        if self._iface:
            try:
                self._iface.close()
            except Exception:
                pass
        self._iface = None
        self._running = False
        time.sleep(1)

    def packets_since(self, since_time, portnum=None):
        with self._lock:
            pkts = [p for p in self.received if p["time"] >= since_time]
        if portnum is not None:
            pkts = [p for p in pkts
                    if p["packet"].get("decoded", {}).get("portnum") == portnum]
        return pkts

    def count_received(self, portnum=None):
        with self._lock:
            if portnum is None:
                return len(self.received)
            return sum(1 for p in self.received
                       if p["packet"].get("decoded", {}).get("portnum") == portnum)

    def find_media_response(self, transfer_id, since_time, timeout=60):
        """Wait for ACK_COMPLETE or NACK for a given transfer_id."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                for entry in self.received:
                    if entry["time"] < since_time:
                        continue
                    pkt = entry["packet"]
                    if pkt.get("decoded", {}).get("portnum") != MEDIA_TRANSFER_APP:
                        continue
                    payload = pkt.get("decoded", {}).get("payload", b"")
                    if isinstance(payload, (bytes, bytearray)) and len(payload) > 0:
                        m = decode_media(payload)
                        if (m["transfer_id"] == transfer_id and
                                m["type"] in (MEDIA_ACK_COMPLETE, MEDIA_NACK)):
                            return m
            time.sleep(1)
        return None


# ---------------------------------------------------------------------------
# Media transfer helper
# ---------------------------------------------------------------------------

def send_media_transfer(from_label, dest_label, data, content_type,
                        mime_type="", duration_seconds=0, width=0, height=0,
                        listener=None, stat_key=None):
    """
    Send a complete media transfer: START -> CHUNKs -> COMPLETE via a single
    persistent serial connection. Listens for ACK_COMPLETE/NACK on the same
    connection (source device sees the ACK coming back over radio).

    Returns (transfer_id, acked) where acked is True if ACK_COMPLETE received.
    """
    transfer_id = random.randint(0x1000, 0xFFFFFFF)
    checksum = crc32(data)
    total_chunks = (len(data) + CHUNK_SIZE - 1) // CHUNK_SIZE

    key = stat_key or f"media_{from_label}_to_{dest_label}"

    log(f"  Media transfer {from_label}->{dest_label}: {len(data)}B, "
        f"{total_chunks} chunks, tid=0x{transfer_id:08X}")

    # Build packet sequence: START, CHUNKs, COMPLETE
    packets = []

    # MEDIA_START
    start_pkt = encode_media(
        type_val=MEDIA_START,
        transfer_id=transfer_id,
        total_chunks=total_chunks,
        total_size=len(data),
        content_type=content_type,
        checksum=checksum,
        mime_type=mime_type,
        duration_seconds=duration_seconds,
        width=width,
        height=height,
    )
    packets.append((start_pkt, MEDIA_DELAY))

    # MEDIA_CHUNKs
    for i in range(total_chunks):
        offset = i * CHUNK_SIZE
        chunk_data = data[offset:min(offset + CHUNK_SIZE, len(data))]
        chunk_pkt = encode_media(
            type_val=MEDIA_CHUNK,
            transfer_id=transfer_id,
            chunk_index=i,
            chunk_data=chunk_data,
        )
        packets.append((chunk_pkt, MEDIA_DELAY))

    # MEDIA_COMPLETE
    complete_pkt = encode_media(
        type_val=MEDIA_COMPLETE,
        transfer_id=transfer_id,
        checksum=checksum,
    )
    packets.append((complete_pkt, 0))  # no delay after last packet; we wait for ACK

    # Send all packets via persistent connection and wait for ACK
    chunks_sent, response = send_media_persistent(
        from_label, dest_label, packets, timeout_for_ack=MEDIA_WAIT)

    stats[key]["sent"] += 1
    if chunks_sent >= total_chunks + 2:  # START + all chunks + COMPLETE
        stats[key]["send_ok"] += 1
    else:
        stats[key]["send_fail"] += 1
        if chunks_sent == 0:
            stats[key]["errors"].append(f"tid=0x{transfer_id:08X} connection failed")
            return transfer_id, False

    acked = False
    stats[key]["recv_expected"] += 1
    if response:
        if response["type"] == MEDIA_ACK_COMPLETE:
            acked = True
            stats[key]["recv_ok"] += 1
            log(f"  ACK_COMPLETE for tid=0x{transfer_id:08X} "
                f"({chunks_sent-2}/{total_chunks} chunks delivered)", "OK")
        else:
            log(f"  NACK for tid=0x{transfer_id:08X} "
                f"(missing={response['missing_chunks']})", "WARN")
    else:
        log(f"  No ACK/NACK for tid=0x{transfer_id:08X} within {MEDIA_WAIT}s "
            f"({chunks_sent-2}/{total_chunks} chunks sent)", "WARN")

    return transfer_id, acked


# ---------------------------------------------------------------------------
# Availability check
# ---------------------------------------------------------------------------

def get_reboot_count(label):
    """Get device reboot count via CLI."""
    port = DEVICES[label]["port"]
    cmd = [MESHTASTIC, "--port", port, "--info"]
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            stdout, _ = proc.communicate(timeout=20)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
            return None
        import re
        m = re.search(r'"rebootCount":\s*(\d+)', stdout)
        return int(m.group(1)) if m else None
    except Exception:
        return None

# Track reboot counts per device across test
reboot_counts = {}  # label -> {"start": N, "current": N, "reboots": N}

def check_devices():
    """Verify all four devices are reachable. Auto-updates node IDs if changed."""
    available = []
    section("Device Availability Check")
    for label, cfg in DEVICES.items():
        port = cfg["port"]
        expected_id = cfg["node_id"]
        if not os.path.exists(port):
            log(f"{label}: port {port} not present — SKIPPING", "WARN")
            continue
        nid = get_node_id(label)
        if nid is None:
            log(f"{label}: port exists but CLI timed out — SKIPPING", "WARN")
            continue
        if nid != expected_id:
            log(f"{label}: node changed 0x{expected_id:08x} -> 0x{nid:08x} — auto-updating", "WARN")
            cfg["node_id"] = nid
            cfg["node_hex"] = f"{nid:08x}"
        # Track reboot count
        rcount = get_reboot_count(label)
        if rcount is not None:
            reboot_counts[label] = {"start": rcount, "current": rcount, "reboots": 0}
            log(f"{label}: OK  node=0x{nid:08x}  rebootCount={rcount}  port={port}", "OK")
        else:
            log(f"{label}: OK  node=0x{nid:08x}  port={port}", "OK")
        available.append(label)
    return available

def check_reboots(available):
    """Check for device reboots since test start. Log any detected."""
    for label in available:
        if label not in reboot_counts:
            continue
        rcount = get_reboot_count(label)
        if rcount is None:
            log(f"{label}: OFFLINE — port lost!", "ERR")
            continue
        prev = reboot_counts[label]["current"]
        if rcount > prev:
            delta = rcount - prev
            total = rcount - reboot_counts[label]["start"]
            reboot_counts[label]["current"] = rcount
            reboot_counts[label]["reboots"] = total
            log(f"{label}: REBOOTED {delta}x (total {total} since test start, rebootCount={rcount})", "ERR")


# ---------------------------------------------------------------------------
# Phase 1: Text Messages
# ---------------------------------------------------------------------------

def phase1_text(available, quick=False, medium=False):
    section("PHASE 1: Text Messages")

    if quick:
        dm_per_pair, broadcasts_each, channel_each = 10, 5, 3
    elif medium:
        dm_per_pair, broadcasts_each, channel_each = 50, 15, 10
    else:
        dm_per_pair, broadcasts_each, channel_each = 100, 25, 15

    phase_stats = {
        "dm_total_attempted": 0,
        "dm_total_sent": 0,
        "broadcast_attempted": 0,
        "broadcast_sent": 0,
        "channel_attempted": 0,
        "channel_sent": 0,
    }

    # --- 1a. DMs between each pair ---
    subsection(f"Phase 1a: DMs ({dm_per_pair} per pair, {len(ALL_PAIRS)} pairs)")

    for (src, dst) in ALL_PAIRS:
        if src not in available or dst not in available:
            log(f"Skipping {src}->{dst}: one or both devices unavailable", "WARN")
            continue

        key = f"text_dm_{src}_to_{dst}"
        sent_ok = 0
        log(f"  {src} -> {dst}: sending {dm_per_pair} DMs")
        for i in range(dm_per_pair):
            tag = f"dm{i:04d}_{src}_{dst}_{int(time.time())&0xFFFF}"
            ok = send_text_cli(src, dst, tag)
            stats[key]["sent"] += 1
            phase_stats["dm_total_attempted"] += 1
            if ok:
                stats[key]["send_ok"] += 1
                sent_ok += 1
                phase_stats["dm_total_sent"] += 1
            else:
                stats[key]["send_fail"] += 1
            time.sleep(TEXT_DELAY)

        pct = 100 * sent_ok / dm_per_pair if dm_per_pair else 0
        log(f"  {src}->{dst}: {sent_ok}/{dm_per_pair} sent ({pct:.0f}%)",
            "OK" if sent_ok >= dm_per_pair * 0.9 else "WARN")
        check_reboots(available)

    # --- 1b. Broadcasts ---
    subsection(f"Phase 1b: Broadcasts ({broadcasts_each} per device)")

    for dev in DEVICE_LIST:
        if dev not in available:
            log(f"Skipping {dev} broadcasts: unavailable", "WARN")
            continue
        key = f"text_bcast_{dev}"
        sent_ok = 0
        log(f"  {dev}: sending {broadcasts_each} broadcasts")
        for i in range(broadcasts_each):
            msg = f"bcast{i:04d}_{dev}_{int(time.time())&0xFFFF}"
            ok = send_broadcast_cli(dev, msg)
            stats[key]["sent"] += 1
            phase_stats["broadcast_attempted"] += 1
            if ok:
                stats[key]["send_ok"] += 1
                sent_ok += 1
                phase_stats["broadcast_sent"] += 1
            else:
                stats[key]["send_fail"] += 1
            time.sleep(TEXT_DELAY)

        pct = 100 * sent_ok / broadcasts_each if broadcasts_each else 0
        log(f"  {dev}: {sent_ok}/{broadcasts_each} broadcasts sent ({pct:.0f}%)",
            "OK" if sent_ok >= broadcasts_each * 0.9 else "WARN")

    # --- 1c. Channel messages on ch0 (maluca) ---
    subsection(f"Phase 1c: Channel messages ({channel_each} per device on ch0)")

    for dev in DEVICE_LIST:
        if dev not in available:
            log(f"Skipping {dev} channel msgs: unavailable", "WARN")
            continue
        key = f"text_channel_{dev}"
        sent_ok = 0
        log(f"  {dev}: sending {channel_each} channel messages on ch0")
        for i in range(channel_each):
            msg = f"ch{i:03d}_{dev}_{int(time.time())&0xFFFF}"
            ok = send_channel_cli(dev, msg, channel=0)
            stats[key]["sent"] += 1
            phase_stats["channel_attempted"] += 1
            if ok:
                stats[key]["send_ok"] += 1
                sent_ok += 1
                phase_stats["channel_sent"] += 1
            else:
                stats[key]["send_fail"] += 1
            time.sleep(TEXT_DELAY)

        pct = 100 * sent_ok / channel_each if channel_each else 0
        log(f"  {dev}: {sent_ok}/{channel_each} channel messages sent ({pct:.0f}%)",
            "OK" if sent_ok >= channel_each * 0.9 else "WARN")

    phase_results["phase1"] = phase_stats
    total_attempted = (phase_stats["dm_total_attempted"] +
                       phase_stats["broadcast_attempted"] +
                       phase_stats["channel_attempted"])
    total_sent = (phase_stats["dm_total_sent"] +
                  phase_stats["broadcast_sent"] +
                  phase_stats["channel_sent"])
    pct = 100 * total_sent / total_attempted if total_attempted else 0
    log(f"\nPhase 1 complete: {total_sent}/{total_attempted} messages sent ({pct:.1f}%)",
        "OK" if pct >= 90 else "WARN")
    return phase_stats


# ---------------------------------------------------------------------------
# Phase 2: Voice Memos
# ---------------------------------------------------------------------------

VOICE_DURATIONS = [3, 5, 10]   # seconds

def phase2_voice(available, quick=False):
    section("PHASE 2: Voice Memos (portnum 259)")

    # Build transfer schedule: at least 20 transfers
    # For each pair x each duration = 6 pairs x 3 durations = 18, plus a few extras
    schedule = []
    for (src, dst) in ALL_PAIRS:
        if src not in available or dst not in available:
            continue
        for dur in VOICE_DURATIONS:
            schedule.append((src, dst, dur))

    # Add reversed direction for some pairs to hit 20+
    for (src, dst) in ALL_PAIRS[:4]:
        if dst in available and src in available:
            schedule.append((dst, src, 3))

    if quick:
        schedule = schedule[:6]

    log(f"Scheduled {len(schedule)} voice memo transfers")

    # For voice memos we open a listener on the DESTINATION for each transfer.
    # Because meshtastic pubsub is global, we serialize: one transfer at a time.

    phase_stats = {
        "total_transfers": len(schedule),
        "sent_complete": 0,
        "acked": 0,
        "failed": 0,
    }

    for idx, item in enumerate(schedule):
        src, dst, dur = item
        log(f"\n  [{idx+1}/{len(schedule)}] Voice memo {src}->{dst}: {dur}s")

        data = generate_codec2(dur * 1000)
        key = f"voice_{src}_to_{dst}_{dur}s"

        # Use persistent connection on SOURCE — ACK comes back over radio
        _, acked = send_media_transfer(
            from_label=src,
            dest_label=dst,
            data=data,
            content_type=CT_VOICE_MEMO,
            mime_type="audio/codec2",
            duration_seconds=dur,
            stat_key=key,
        )

        if stats[key]["send_ok"] > 0:
            phase_stats["sent_complete"] += 1
        else:
            phase_stats["failed"] += 1

        if acked:
            phase_stats["acked"] += 1

        time.sleep(5)   # cooldown between transfers

        # Check for reboots periodically
        if (idx + 1) % 5 == 0:
            check_reboots(available)

    phase_results["phase2"] = phase_stats
    pct_sent  = 100 * phase_stats["sent_complete"] / len(schedule) if schedule else 0
    pct_acked = 100 * phase_stats["acked"] / len(schedule) if schedule else 0
    log(f"\nPhase 2 complete: {phase_stats['sent_complete']}/{len(schedule)} transfers sent "
        f"({pct_sent:.0f}%), {phase_stats['acked']} ACKed ({pct_acked:.0f}%)",
        "OK" if pct_sent >= 80 else "WARN")
    return phase_stats


# ---------------------------------------------------------------------------
# Phase 3: Image Transfers
# ---------------------------------------------------------------------------

IMAGE_SIZES = [128, 256, 512]   # bytes (simulated compressed thumbnails)

def phase3_images(available, quick=False):
    section("PHASE 3: Image Transfers (portnum 259)")

    schedule = []
    for (src, dst) in ALL_PAIRS:
        if src not in available or dst not in available:
            continue
        for sz in IMAGE_SIZES:
            schedule.append((src, dst, sz))

    # Add reversed transfers to hit 20+
    for (src, dst) in ALL_PAIRS[:4]:
        if dst in available and src in available:
            schedule.append((dst, src, 256))

    if quick:
        schedule = schedule[:6]

    log(f"Scheduled {len(schedule)} image transfers")

    phase_stats = {
        "total_transfers": len(schedule),
        "sent_complete": 0,
        "acked": 0,
        "failed": 0,
    }

    for idx, item in enumerate(schedule):
        src, dst, size = item
        log(f"\n  [{idx+1}/{len(schedule)}] Image {src}->{dst}: {size}B")

        data = generate_test_image(size)
        # Make dims proportional to size for realism
        w = 16 * (size // 128)
        h = 16 * (size // 128)
        key = f"image_{src}_to_{dst}_{size}B"

        # Use persistent connection on SOURCE — ACK comes back over radio
        _, acked = send_media_transfer(
            from_label=src,
            dest_label=dst,
            data=data,
            content_type=CT_IMAGE_THUMBNAIL,
            mime_type="image/jpeg",
            width=w,
            height=h,
            stat_key=key,
        )

        if stats[key]["send_ok"] > 0:
            phase_stats["sent_complete"] += 1
        else:
            phase_stats["failed"] += 1

        if acked:
            phase_stats["acked"] += 1

        time.sleep(5)

        # Check for reboots periodically
        if (idx + 1) % 5 == 0:
            check_reboots(available)

    phase_results["phase3"] = phase_stats
    pct_sent  = 100 * phase_stats["sent_complete"] / len(schedule) if schedule else 0
    pct_acked = 100 * phase_stats["acked"] / len(schedule) if schedule else 0
    log(f"\nPhase 3 complete: {phase_stats['sent_complete']}/{len(schedule)} transfers sent "
        f"({pct_sent:.0f}%), {phase_stats['acked']} ACKed ({pct_acked:.0f}%)",
        "OK" if pct_sent >= 80 else "WARN")
    return phase_stats


# ---------------------------------------------------------------------------
# Phase 4: Mixed Traffic Stress
# ---------------------------------------------------------------------------

def phase4_mixed(available, quick=False):
    section("PHASE 4: Mixed Traffic Stress")

    if len(available) < 2:
        log("Need at least 2 devices for mixed stress — skipping", "WARN")
        phase_results["phase4"] = {"skipped": True}
        return

    burst_count  = 5  if quick else 20
    long_count   = 3  if quick else 10
    rapid_count  = 5  if quick else 20
    mixed_rounds = 3  if quick else 10

    phase_stats = {
        "burst_sent": 0,
        "burst_attempted": 0,
        "long_sent": 0,
        "long_attempted": 0,
        "rapid_sent": 0,
        "rapid_attempted": 0,
        "mixed_sent": 0,
        "mixed_attempted": 0,
    }

    pairs_avail = [(s, d) for (s, d) in ALL_PAIRS if s in available and d in available]
    if not pairs_avail:
        log("No valid pairs available for stress — skipping", "WARN")
        phase_results["phase4"] = {"skipped": True}
        return

    # --- 4a. Rapid-fire burst (0.5s intervals) ---
    subsection(f"Phase 4a: Rapid-fire burst ({burst_count} messages, 0.5s gap)")
    src, dst = pairs_avail[0]
    log(f"  {src} -> {dst}: {burst_count} rapid-fire texts")
    for i in range(burst_count):
        msg = f"burst{i:04d}_{int(time.time())&0xFFFF}"
        ok = send_text_cli(src, dst, msg, timeout=15)
        phase_stats["burst_attempted"] += 1
        if ok:
            phase_stats["burst_sent"] += 1
        time.sleep(BURST_DELAY)

    pct = 100 * phase_stats["burst_sent"] / phase_stats["burst_attempted"]
    log(f"  Burst: {phase_stats['burst_sent']}/{phase_stats['burst_attempted']} sent ({pct:.0f}%)",
        "OK" if pct >= 70 else "WARN")
    time.sleep(5)

    # --- 4b. Long messages (200+ chars) ---
    subsection(f"Phase 4b: Long messages ({long_count} x 200 chars)")
    pairs_cycle = pairs_avail * ((long_count // len(pairs_avail)) + 1)
    for i in range(long_count):
        src, dst = pairs_cycle[i]
        # 200-char message (max LoRa text payload)
        msg = f"LONG{i:04d}_" + ("X" * 190)
        ok = send_text_cli(src, dst, msg[:200])
        phase_stats["long_attempted"] += 1
        if ok:
            phase_stats["long_sent"] += 1
        time.sleep(TEXT_DELAY)

    pct = 100 * phase_stats["long_sent"] / phase_stats["long_attempted"]
    log(f"  Long msgs: {phase_stats['long_sent']}/{phase_stats['long_attempted']} ({pct:.0f}%)",
        "OK" if pct >= 80 else "WARN")
    time.sleep(5)

    # --- 4c. Rapid multi-device simultaneous sends ---
    subsection(f"Phase 4c: Rapid multi-device simultaneous ({rapid_count} rounds)")
    # Each round: all available devices send at ~same time (serialized due to serial port)
    log(f"  {len(available)} devices, {rapid_count} rounds")
    for rnd in range(rapid_count):
        for dev in available:
            # Pick a random destination that is not self
            others = [d for d in available if d != dev]
            if not others:
                continue
            dst = random.choice(others)
            msg = f"rp{rnd:03d}_{dev}_{int(time.time())&0xFFFF}"
            ok = send_text_cli(dev, dst, msg, timeout=20)
            phase_stats["rapid_attempted"] += 1
            if ok:
                phase_stats["rapid_sent"] += 1
            time.sleep(BURST_DELAY)
        time.sleep(TEXT_DELAY)   # brief cooldown between rounds

    pct = 100 * phase_stats["rapid_sent"] / phase_stats["rapid_attempted"] if phase_stats["rapid_attempted"] else 0
    log(f"  Rapid multi-device: {phase_stats['rapid_sent']}/{phase_stats['rapid_attempted']} ({pct:.0f}%)",
        "OK" if pct >= 70 else "WARN")
    time.sleep(5)

    # --- 4d. Mixed text + media interleaved ---
    subsection(f"Phase 4d: Mixed text+media interleaved ({mixed_rounds} rounds)")
    for rnd in range(mixed_rounds):
        src, dst = pairs_cycle[rnd % len(pairs_avail)]
        log(f"  Round {rnd+1}/{mixed_rounds}: text+media from {src}->{dst}")

        # Text
        msg = f"mixed{rnd:03d}_{int(time.time())&0xFFFF}"
        ok = send_text_cli(src, dst, msg)
        phase_stats["mixed_attempted"] += 1
        if ok: phase_stats["mixed_sent"] += 1
        time.sleep(TEXT_DELAY)

        # Small voice memo (1s = 100 bytes = 1 chunk)
        data = generate_codec2(1000)
        start_pkt = encode_media(
            type_val=MEDIA_START,
            transfer_id=random.randint(0x10000, 0xFFFFF),
            total_chunks=1,
            total_size=len(data),
            content_type=CT_VOICE_MEMO,
            checksum=crc32(data),
            mime_type="audio/codec2",
            duration_seconds=1,
        )
        ok = send_custom_packet(src, start_pkt, MEDIA_TRANSFER_APP, dst)
        phase_stats["mixed_attempted"] += 1
        if ok: phase_stats["mixed_sent"] += 1
        time.sleep(MEDIA_DELAY)

        # Chunk
        chunk_pkt = encode_media(
            type_val=MEDIA_CHUNK,
            transfer_id=0x10000 + rnd,
            chunk_index=0,
            chunk_data=data,
        )
        ok = send_custom_packet(src, chunk_pkt, MEDIA_TRANSFER_APP, dst)
        if ok: phase_stats["mixed_sent"] += 1
        phase_stats["mixed_attempted"] += 1
        time.sleep(MEDIA_DELAY)

        # Complete
        complete_pkt = encode_media(
            type_val=MEDIA_COMPLETE,
            transfer_id=0x10000 + rnd,
            checksum=crc32(data),
        )
        ok = send_custom_packet(src, complete_pkt, MEDIA_TRANSFER_APP, dst)
        if ok: phase_stats["mixed_sent"] += 1
        phase_stats["mixed_attempted"] += 1
        time.sleep(TEXT_DELAY)

    pct = 100 * phase_stats["mixed_sent"] / phase_stats["mixed_attempted"] if phase_stats["mixed_attempted"] else 0
    log(f"  Mixed: {phase_stats['mixed_sent']}/{phase_stats['mixed_attempted']} ({pct:.0f}%)",
        "OK" if pct >= 80 else "WARN")

    phase_results["phase4"] = phase_stats
    total_att  = sum(phase_stats[k] for k in phase_stats if k.endswith("_attempted"))
    total_sent = sum(phase_stats[k] for k in phase_stats if k.endswith("_sent"))
    pct = 100 * total_sent / total_att if total_att else 0
    log(f"\nPhase 4 complete: {total_sent}/{total_att} packets sent ({pct:.1f}%)",
        "OK" if pct >= 75 else "WARN")
    return phase_stats


# ---------------------------------------------------------------------------
# Summary and results output
# ---------------------------------------------------------------------------

def print_summary(available, duration_s):
    section("RESULTS SUMMARY")

    print(f"  Run duration   : {duration_s:.0f}s ({duration_s/60:.1f} min)")
    print(f"  Devices tested : {', '.join(available)}")
    print()

    # Per-stat-key table
    print(f"  {'Key':<45} {'Sent':>6} {'OK':>6} {'Fail':>6}  {'%OK':>5}")
    print(f"  {'─'*45} {'──────':>6} {'──────':>6} {'──────':>6}  {'─────':>5}")

    for key in sorted(stats.keys()):
        s = stats[key]
        sent = s["sent"]
        ok   = s["send_ok"]
        fail = s["send_fail"]
        pct  = 100 * ok / sent if sent else 0
        flag = "" if pct >= 90 else (" WARN" if pct >= 70 else " FAIL")
        print(f"  {key:<45} {sent:>6} {ok:>6} {fail:>6}  {pct:>4.0f}%{flag}")

    print()

    # Per-phase summary
    print(f"  {'─'*68}")
    print(f"  PHASE SUMMARIES")
    print(f"  {'─'*68}")

    p1 = phase_results.get("phase1", {})
    if p1:
        total_att  = p1.get("dm_total_attempted",0) + p1.get("broadcast_attempted",0) + p1.get("channel_attempted",0)
        total_sent = p1.get("dm_total_sent",0)      + p1.get("broadcast_sent",0)       + p1.get("channel_sent",0)
        pct = 100 * total_sent / total_att if total_att else 0
        print(f"  Phase 1 (Text): {total_sent}/{total_att} messages sent ({pct:.1f}%)")
        print(f"    DMs:       {p1.get('dm_total_sent',0)}/{p1.get('dm_total_attempted',0)}")
        print(f"    Broadcasts:{p1.get('broadcast_sent',0)}/{p1.get('broadcast_attempted',0)}")
        print(f"    Channel:   {p1.get('channel_sent',0)}/{p1.get('channel_attempted',0)}")

    p2 = phase_results.get("phase2", {})
    if p2 and not p2.get("skipped"):
        n = p2.get("total_transfers", 0)
        sent  = p2.get("sent_complete", 0)
        acked = p2.get("acked", 0)
        pct_s = 100 * sent  / n if n else 0
        pct_a = 100 * acked / n if n else 0
        print(f"  Phase 2 (Voice Memos): {sent}/{n} sent ({pct_s:.0f}%), {acked} ACKed ({pct_a:.0f}%)")

    p3 = phase_results.get("phase3", {})
    if p3 and not p3.get("skipped"):
        n = p3.get("total_transfers", 0)
        sent  = p3.get("sent_complete", 0)
        acked = p3.get("acked", 0)
        pct_s = 100 * sent  / n if n else 0
        pct_a = 100 * acked / n if n else 0
        print(f"  Phase 3 (Images):      {sent}/{n} sent ({pct_s:.0f}%), {acked} ACKed ({pct_a:.0f}%)")

    p4 = phase_results.get("phase4", {})
    if p4 and not p4.get("skipped"):
        total_att  = sum(p4[k] for k in p4 if k.endswith("_attempted"))
        total_sent = sum(p4[k] for k in p4 if k.endswith("_sent"))
        pct = 100 * total_sent / total_att if total_att else 0
        print(f"  Phase 4 (Mixed):       {total_sent}/{total_att} packets sent ({pct:.1f}%)")
        print(f"    Burst:     {p4.get('burst_sent',0)}/{p4.get('burst_attempted',0)}")
        print(f"    Long msgs: {p4.get('long_sent',0)}/{p4.get('long_attempted',0)}")
        print(f"    Rapid:     {p4.get('rapid_sent',0)}/{p4.get('rapid_attempted',0)}")
        print(f"    Mixed:     {p4.get('mixed_sent',0)}/{p4.get('mixed_attempted',0)}")

    # Reboot summary
    if reboot_counts:
        print(f"\n  {'─'*68}")
        print(f"  DEVICE REBOOTS")
        print(f"  {'─'*68}")
        for label, rc in reboot_counts.items():
            total = rc["reboots"]
            flag = "ERR" if total > 0 else "OK"
            sym = " **" if total > 0 else ""
            print(f"  {label:<8}: {total} reboots (count {rc['start']} -> {rc['current']}){sym}")

    print(f"\n{'='*68}")


def save_results(available, duration_s):
    """Save all results to JSON."""
    results_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "four_device_stress_results.json")

    output = {
        "timestamp": datetime.now().isoformat(),
        "duration_seconds": round(duration_s, 1),
        "devices": {
            label: {
                "port": DEVICES[label]["port"],
                "node_id": f"0x{DEVICES[label]['node_id']:08x}",
                "available": label in available,
            }
            for label in DEVICES
        },
        "per_key_stats": {
            key: {k: v for k, v in s.items() if k != "errors"}
            for key, s in stats.items()
        },
        "per_key_errors": {
            key: s["errors"]
            for key, s in stats.items()
            if s["errors"]
        },
        "phase_results": {k: v for k, v in phase_results.items()},
        "reboot_counts": {k: v for k, v in reboot_counts.items()},
    }

    with open(results_path, "w") as f:
        json.dump(output, f, indent=2)

    log(f"Results saved to {results_path}", "OK")
    return results_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global start_time

    parser = argparse.ArgumentParser(
        description="MeshReliable Four-Device VHF Stress Test")
    parser.add_argument("--phase", "-p", type=int, choices=[1, 2, 3, 4],
                        help="Run only a specific phase (default: all)")
    parser.add_argument("--quick", "-q", action="store_true",
                        help="Reduced message counts for smoke test")
    parser.add_argument("--medium", "-m", action="store_true",
                        help="Medium message counts (50 DMs/pair) for statistical validity")
    parser.add_argument("--loop", "-l", action="store_true",
                        help="Loop continuously until stopped (for endurance testing)")
    args = parser.parse_args()

    print()
    print("=" * 68)
    print("  MeshReliable — Four-Device 144 MHz VHF Stress Test")
    print("=" * 68)
    print(f"  Date     : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    mode = 'QUICK' if args.quick else ('MEDIUM' if args.medium else 'FULL')
    if args.loop:
        mode += ' (LOOP)'
    print(f"  Mode     : {mode}")
    if args.phase:
        print(f"  Phase    : {args.phase} only")
    else:
        print(f"  Phases   : 1 (text) + 2 (voice) + 3 (images) + 4 (mixed)")
    print()
    for label, cfg in DEVICES.items():
        print(f"  {label:<8}: node=!{cfg['node_hex']}  port={cfg['port']}")
    print()

    start_time = time.time()

    # Check connectivity
    available = check_devices()
    if len(available) == 0:
        print("\nERROR: No devices reachable. Aborting.")
        return 1
    if len(available) < 4:
        log(f"Only {len(available)}/4 devices available: {available}", "WARN")
        log("Continuing with available devices — some pair tests will be skipped", "WARN")

    # Run phases
    run_all = args.phase is None
    iteration = 0

    while True:
        iteration += 1
        if args.loop and iteration > 1:
            section(f"LOOP ITERATION {iteration}")
            # Re-check devices in case ports changed
            available = check_devices()
            if len(available) == 0:
                log("No devices reachable — waiting 30s and retrying...", "ERR")
                time.sleep(30)
                continue

        if run_all or args.phase == 1:
            phase1_text(available, quick=args.quick, medium=args.medium)
            check_reboots(available)

        if run_all or args.phase == 2:
            phase2_voice(available, quick=args.quick)
            check_reboots(available)

        if run_all or args.phase == 3:
            phase3_images(available, quick=args.quick)
            check_reboots(available)

        if run_all or args.phase == 4:
            phase4_mixed(available, quick=args.quick)
            check_reboots(available)

        duration_s = time.time() - start_time
        print_summary(available, duration_s)
        results_path = save_results(available, duration_s)
        print(f"\n  Results: {results_path}")
        print()

        if not args.loop:
            break

        log(f"Loop iteration {iteration} complete ({duration_s/60:.1f} min elapsed). "
            f"Cooldown 30s before next iteration...")
        time.sleep(30)

    # Exit code: non-zero if any stat key has send rate < 70%
    any_fail = False
    for key, s in stats.items():
        if s["sent"] > 0 and (s["send_ok"] / s["sent"]) < 0.70:
            any_fail = True
            break

    return 1 if any_fail else 0


if __name__ == "__main__":
    sys.exit(main())
