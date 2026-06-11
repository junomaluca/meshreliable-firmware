#!/usr/bin/env python3
"""
Comprehensive 7-Device MeshReliable End-to-End Test
====================================================
Tests text DMs, voice memos, image transfers, group messages, and channel
broadcasts across 5 USB-connected devices + 2 remote devices, with iOS app
monitoring via DebugHTTPServer.

Phases:
  1. Setup — connect devices, discover IDs, find maluca channel, verify iOS
  2. Text DMs (25 msgs) — round-robin targets including remotes
  3. Voice DMs (25 msgs) — Codec2 media transfer protocol
  4. Image DMs (25 msgs) — thumbnail media transfer protocol
  5. Group setup — build 7-member group from maluca channel
  6. Group text (25 msgs) — GROUP_TEXT on portnum 258
  7. Group voice (25 msgs) — voice via media transfer, broadcast
  8. Group image (25 msgs) — image via media transfer, broadcast
  9. Channel broadcasts (25 msgs) — maluca channel broadcasts

Usage:
  python3 -u tests/full_7device_test.py 2>&1 | tee /tmp/full_7device_test.log
"""

import sys
import os
import time
import json
import random
import threading
import traceback
import signal
import collections
import hashlib
from datetime import datetime

# Prevent meshtastic library from calling sys.exit on serial errors
_real_exit = sys.exit
def _guarded_exit(code=0):
    if code != 0:
        raise SystemExit(code)
    _real_exit(code)
sys.exit = _guarded_exit

signal.signal(signal.SIGHUP, signal.SIG_IGN)

try:
    import requests
except ImportError:
    requests = None

import meshtastic
import meshtastic.serial_interface
from pubsub import pub

# ─── Configuration ───────────────────────────────────────────────────────

USB_DEVICES = {
    # Ports here are only initial hints — discovery re-assigns by node number across all ports.
    "Pager":  {"port": "/dev/cu.usbmodem313101", "id": None, "hw": "T_LORA_PAGER"},
    "VHF":    {"port": "/dev/cu.usbmodem1101",   "id": None, "hw": "TBEAM_S3_CORE"},  # VHF-A recovered 2026-06-10
    "BPF":    {"port": "/dev/cu.usbmodem101",    "id": None, "hw": "TBEAM_BPF"},
    "T3S3":   {"port": "/dev/cu.usbmodem21201",  "id": None, "hw": "TLORA_T3_S3"},
    "XIAO":   {"port": "/dev/cu.usbmodem31201",  "id": None, "hw": "SEEED_XIAO_S3"},
}

# Stable hardware deviceIds (efuse identity, churn-proof) — for re-verification:
#   101   k8h18KKJGDBJVOWP7PJvow==  Pager A   (tlora-pager-lr1121, 915)
#   1101  1hGLsQdl9lrTMYgD1+j1Yg==  VHF-A     (tbeam-s3-core, 144 VHF)
#   21101 oUEg9bb6Uye1KuWlK2Xemg==  BPF A     (tbeam-bpf, 144 VHF)
#   21201 8JKkFdMGrt2kKPEzA7ndLg==  T3S3      (tlora-t3s3-v1, 915)
#   31201 rYmnF+L3pfGUJXjuaH5L8A==  XIAO A    (seeed-xiao-s3, 915)
#   BPF B sS7S/uUlHVmSxqHSJZ77Mg==  OFFLINE-USB (tbeam-bpf) — radio-only if powered

REMOTE_DEVICES = {
    "VHF-Remote": {"id": None, "id_contains": "1bdc", "hw": "TBEAM_S3_CORE"},
    "BPF-Remote": {"id": None, "id_contains": "c605", "hw": "TBEAM_BPF"},
}

ALL_DEVICE_NAMES = list(USB_DEVICES.keys()) + list(REMOTE_DEVICES.keys())

IOS_API_BASE = "http://192.168.1.187:8765"
IOS_FALLBACK = "http://localhost:8765"

MSGS_PER_PHASE = 25

# Timing — keep realistic but not wasteful
DM_TIMEOUT = 15          # seconds to wait for DM receipt
MEDIA_CHUNK_DELAY = 8    # seconds between media chunks (< 8 causes serial port instability)
MEDIA_ACK_TIMEOUT = 30   # seconds to wait for ACK_COMPLETE
MEDIA_MAX_ATTEMPTS = 5   # retransmit the whole transfer up to this many times until ACK_COMPLETE
MEDIA_ATTEMPT_TIMEOUT = 20  # seconds to wait for ACK_COMPLETE per attempt before retransmitting
BROADCAST_TIMEOUT = 10   # seconds for broadcast verification
INTER_MSG_DELAY = 1      # seconds between messages in a phase
# MEDIA_SEND_DELAY: extra spacing between media transfers (media is many packets — like group,
# rapid-fire saturates the channel). Env-overridable for realistic cadence.
MEDIA_SEND_DELAY = int(os.environ.get("MEDIA_SEND_DELAY", "2"))
GROUP_MSG_WAIT = 10       # seconds to wait for group msg delivery
SEND_HANG_TIMEOUT = 20   # seconds before a send() is treated as hung (e.g. wedged pager USB-TX)
MAX_RECONNECTS = 4       # give up on a chronically-dropping device after this many reconnects/phase-run
# Known-flaky USB ports to skip in discovery (device is on the mesh via radio, but its
# USB-CDC handshake won't complete and floods parse errors that jam setup). VHF-A
# (tbeam-s3-core, 1101) has a chronic CDC handshake issue post-factory-flash.
import os as _os
SKIP_USB_PORTS = set(_os.environ.get("SKIP_PORTS","/dev/cu.usbmodem1101").split(",")) if _os.environ.get("SKIP_PORTS") is not None else {"/dev/cu.usbmodem1101"}
RETRY_RECONCILE_GRACE = 300  # DM-A: success measured 5 MIN after send (MeshReliable persistent retry).
                             # After each phase, wait this long and upgrade messages the retries
                             # delivered after the per-message timeout. The last-sent message in a phase
                             # gets a full 5 min; earlier ones get phase-duration + 5 min.

# Stable owner longNames per device — used for identity-based (re)discovery across ports.
NAME_TO_LONGNAME = {
    "Pager": "Pager A", "VHF": "Supreme A", "BPF": "BPF A",
    "T3S3": "T3S3-1", "XIAO": "XIAO A",
}

# Portnums
PORTNUM_TEXT = 1
PORTNUM_GROUP = 258
PORTNUM_MEDIA = 259

# Output
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "e2e_results")
_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
JSONL_FILE = os.path.join(LOG_DIR, f"full_7device_{_ts}.jsonl")
REPORT_FILE = os.path.join(LOG_DIR, f"full_7device_{_ts}_report.md")

# ANSI colors
C = {"g": "\033[92m", "y": "\033[93m", "r": "\033[91m", "b": "\033[1m",
     "d": "\033[2m", "0": "\033[0m"}


# ─── Protobuf Helpers ───────────────────────────────────────────────────

def _ev(v):
    """Encode varint."""
    buf = bytearray()
    if v == 0:
        buf.append(0)
        return buf
    while v > 0x7F:
        buf.append((v & 0x7F) | 0x80)
        v >>= 7
    buf.append(v & 0x7F)
    return buf

def _fv(fn, val):
    """Encode field varint (wire type 0). Omits zero values."""
    if val == 0:
        return bytearray()
    return _ev((fn << 3) | 0) + _ev(val)

def _fv_force(fn, val):
    """Encode field varint even if zero (for type=GROUP_TEXT which is 0)."""
    return _ev((fn << 3) | 0) + _ev(val)

def _fb(fn, val):
    """Encode field bytes (wire type 2). Omits empty values."""
    if not val:
        return bytearray()
    if isinstance(val, str):
        val = val.encode("utf-8")
    return _ev((fn << 3) | 2) + _ev(len(val)) + bytearray(val)

def _fp(fn, values):
    """Encode packed repeated uint32 (wire type 2)."""
    if not values:
        return bytearray()
    packed = bytearray()
    for v in values:
        packed.extend(_ev(v))
    return _ev((fn << 3) | 2) + _ev(len(packed)) + packed

def _crc32(data):
    """IEEE 802.3 CRC32 matching firmware."""
    crc = 0xFFFFFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ 0xEDB88320 if crc & 1 else crc >> 1
    return (~crc) & 0xFFFFFFFF

def _dec_fields(payload):
    """Decode all protobuf fields into a dict {field_num: value}."""
    fields = {}
    if not isinstance(payload, (bytes, bytearray)) or not payload:
        return fields
    pos = 0
    while pos < len(payload):
        tw = 0; s = 0
        while pos < len(payload):
            b = payload[pos]; pos += 1; tw |= (b & 0x7F) << s; s += 7
            if not (b & 0x80):
                break
        fn = tw >> 3; wt = tw & 7
        if wt == 0:
            v = 0; s = 0
            while pos < len(payload):
                b = payload[pos]; pos += 1; v |= (b & 0x7F) << s; s += 7
                if not (b & 0x80):
                    break
            fields[fn] = v
        elif wt == 2:
            ln = 0; s = 0
            while pos < len(payload):
                b = payload[pos]; pos += 1; ln |= (b & 0x7F) << s; s += 7
                if not (b & 0x80):
                    break
            fields[fn] = payload[pos:pos + ln]; pos += ln
        elif wt == 5:
            fields[fn] = payload[pos:pos + 4]; pos += 4
        elif wt == 1:
            fields[fn] = payload[pos:pos + 8]; pos += 8
        else:
            break
    return fields


# ─── Logging ─────────────────────────────────────────────────────────────

def log(msg, level="INFO", end="\n"):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] [{level}] {msg}"
    print(line, end=end, flush=True)


def run_with_timeout(fn, timeout):
    """Run fn() in a daemon thread; return (result, ok). ok=False if it hung or raised.
    A hung thread is abandoned (leaks) but the caller continues — needed because a
    wedged tbeam serial port can make SerialInterface()/close() block forever."""
    box = {}
    def _r():
        try:
            box['v'] = fn()
        except Exception as e:
            box['e'] = e
    t = threading.Thread(target=_r, daemon=True)
    t.start(); t.join(timeout)
    if t.is_alive() or 'e' in box:
        return None, False
    return box.get('v'), True


def guard_iface_sends(iface, name):
    """Wrap sendText/sendData with a watchdog so a wedged serial (e.g. pager USB-TX
    hang) returns instead of stalling the whole test. A hung send returns None;
    the device then fails its message and the consec-failure logic skips it."""
    for meth in ("sendText", "sendData"):
        orig = getattr(iface, meth)
        def make(orig, meth):
            def guarded(*a, **k):
                box = {}
                def run():
                    try:
                        box['r'] = orig(*a, **k)
                    except Exception as e:
                        box['e'] = e
                t = threading.Thread(target=run, daemon=True)
                t.start(); t.join(SEND_HANG_TIMEOUT)
                if t.is_alive():
                    log(f"{name}.{meth} hung >{SEND_HANG_TIMEOUT}s — treated as failed", "WARN")
                    return None
                if 'e' in box:
                    raise box['e']
                return box.get('r')
            return guarded
        setattr(iface, meth, make(orig, meth))


# ─── iOS API Client ─────────────────────────────────────────────────────

class iOSClient:
    """Minimal HTTP client for DebugHTTPServer on iPhone."""

    def __init__(self):
        self.base_url = None
        self.available = False

    def connect(self):
        if requests is None:
            log("requests not installed — iOS monitoring disabled", "WARN")
            return False

        for url in [IOS_API_BASE, IOS_FALLBACK]:
            try:
                r = requests.get(f"{url}/status", timeout=3)
                if r.status_code == 200:
                    self.base_url = url
                    self.available = True
                    status = r.json()
                    log(f"iOS app connected at {url} — status: {status.get('status', '?')}")
                    return True
            except Exception:
                continue

        log("iOS app not reachable — iOS monitoring disabled", "WARN")
        return False

    def get_messages(self, limit=20, user_num=None):
        if not self.available:
            return []
        try:
            params = {"limit": limit}
            if user_num is not None:
                params["userNum"] = user_num
            r = requests.get(f"{self.base_url}/messages", params=params, timeout=5)
            return r.json() if r.status_code == 200 else []
        except Exception:
            return []

    def get_nodes(self):
        if not self.available:
            return []
        try:
            r = requests.get(f"{self.base_url}/nodes", timeout=5)
            return r.json() if r.status_code == 200 else []
        except Exception:
            return []

    def get_groups(self):
        if not self.available:
            return []
        try:
            r = requests.get(f"{self.base_url}/groups", timeout=5)
            return r.json() if r.status_code == 200 else []
        except Exception:
            return []

    def check_message(self, tracking_id, timeout=5):
        """Poll for a message containing tracking_id."""
        if not self.available:
            return None
        deadline = time.time() + timeout
        while time.time() < deadline:
            msgs = self.get_messages(limit=20)
            for msg in msgs:
                text = msg.get("text", "")
                if tracking_id in text:
                    return msg
            time.sleep(1)
        return None


# ─── Test Harness ────────────────────────────────────────────────────────

class Full7DeviceTest:
    def __init__(self):
        self.interfaces = {}          # name -> SerialInterface
        self.iface_to_name = {}       # id(iface) -> name
        self.all_devices = {}         # name -> {"id": int, ...}
        self.lock = threading.Lock()
        self.running = True

        # Pending receive verifications
        self.pending_rx = {}          # tracking_id -> {event, sender, expect_receiver, received_by, ...}
        self.pending_ack = {}         # transfer_id -> {event, result, ...}

        # Results
        self.results = []             # list of per-message dicts
        self.phase_stats = collections.defaultdict(lambda: {"sent": 0, "ok": 0, "fail": 0})

        # Maluca channel
        self.maluca_idx = None
        self.maluca_hash = None

        # iOS
        self.ios = iOSClient()

        # Consecutive failures per device
        self.consec_fail = collections.defaultdict(int)
        self.skip_devices = set()
        self.reconnect_count = collections.defaultdict(int)
        # DM-A: persistent record of which message tags were actually received (by whom),
        # so deliveries that arrive AFTER the per-message timeout (via firmware retries)
        # can still be counted in a post-phase reconciliation pass.
        self.delivered_tags = collections.defaultdict(set)  # tag -> set(receiver names)
        self.tracked_tags = set()                            # every tag we've sent this run
        # Group-message ACK tracking: a member is "delivered" when it returns a GROUP_ACK.
        self.group_acks = collections.defaultdict(set)       # msg_id -> set(member node IDs that ACKed)
        self.group_msg_meta = {}                             # tag -> {msg_id, src_id, online: set(node IDs)}

    # ─── Setup ───────────────────────────────────────────────────────

    def setup(self):
        os.makedirs(LOG_DIR, exist_ok=True)

        print(f"\n{'='*70}")
        print(f"  COMPREHENSIVE 7-DEVICE MESHRELIABLE TEST")
        print(f"  {MSGS_PER_PHASE} messages per phase, 9 phases")
        print(f"  Output: {JSONL_FILE}")
        print(f"{'='*70}\n")

        # Subscribe to receive callbacks
        pub.subscribe(self._on_rx, "meshtastic.receive")
        pub.subscribe(self._on_disconnect, "meshtastic.connection.lost")

        # Connect USB devices BY IDENTITY (longName) across ALL ports — robust to
        # port re-enumeration from replugs (ports AND node-nums shuffle; only the
        # owner longName / deviceId are stable). No hardcoded port is trusted.
        import glob as _glob
        # Match by STABLE node number first (churn-proof, MAC-anchored) — names have drifted
        # (VHF-A renamed "VHF-A", BPF A briefly "Supreme A"), so longName is only a fallback.
        usb_num_to_name = {
            0x5c1a0bc9: "Pager",  # Pager A
            0x335e1be8: "VHF",    # VHF-A (Supreme A)
            0x16d3ef94: "BPF",    # BPF A
            0x61741de3: "T3S3",   # T3S3-1
            0x5c68527e: "XIAO",   # XIAO A
        }
        usb_longname_to_name = {
            "pager a": "Pager", "supreme a": "VHF", "vhf-a": "VHF", "bpf a": "BPF",
            "t3s3-1": "T3S3", "xiao a": "XIAO",
        }
        ports = sorted(_glob.glob("/dev/cu.usbmodem*"))
        log(f"Scanning {len(ports)} USB ports by device identity...")
        for port in ports:
            if port in SKIP_USB_PORTS:
                log(f"  {port}: SKIPPING (known-flaky USB-CDC; device is on the mesh via radio)", "WARN")
                continue
            # Retry probe — these radios often time out on the FIRST connect attempt
            # (esp. the Pager) then answer on the second. A single probe dropped them.
            connected = False
            for attempt in range(1, 4):
                iface = None
                try:
                    iface = meshtastic.serial_interface.SerialInterface(port)
                    time.sleep(3)
                    node = iface.getMyNodeInfo()
                    num = node.get("num", 0)
                    ln = (node.get("user", {}).get("longName") or "").strip()
                    name = usb_num_to_name.get(num) or usb_longname_to_name.get(ln.lower())
                    if not name:
                        log(f"  {port}: '{ln}' not a known USB test device — skipping", "WARN")
                        iface.close(); break
                    if name in self.interfaces:
                        iface.close(); break
                    USB_DEVICES.setdefault(name, {"hw": "?"})["port"] = port
                    USB_DEVICES[name]["id"] = num
                    guard_iface_sends(iface, name)
                    self.interfaces[name] = iface
                    self.iface_to_name[id(iface)] = name
                    self.all_devices[name] = {"id": num, "usb": True}
                    hw = node.get("user", {}).get("hwModel", "?")
                    log(f"  {name}: {port}  0x{num:08x} hw={hw} ('{ln}'){' (retry '+str(attempt)+')' if attempt>1 else ''}")
                    connected = True
                    break
                except Exception as e:
                    try:
                        if iface: iface.close()
                    except Exception:
                        pass
                    if attempt < 3:
                        time.sleep(2)
                    else:
                        log(f"  {port}: probe failed/unresponsive after {attempt} tries — {e}", "WARN")

        time.sleep(3)

        # Discover remote devices from nodeDB
        self._discover_remotes()

        # Find maluca channel
        self._find_maluca_channel()

        # Connect iOS
        self.ios.connect()

        active_usb = [n for n in USB_DEVICES if n in self.interfaces]
        remote = [n for n in REMOTE_DEVICES if REMOTE_DEVICES[n]["id"] is not None]
        log(f"Active USB: {', '.join(active_usb)} ({len(active_usb)}/5)")
        log(f"Remote: {', '.join(remote)} ({len(remote)}/2)")
        log(f"Maluca channel index: {self.maluca_idx}")
        log(f"iOS monitoring: {'ON' if self.ios.available else 'OFF'}")

        if len(active_usb) < 2:
            log("Need at least 2 USB devices", "ERROR")
            return False

        # Warmup — small media transfer to prime radio
        names = list(self.interfaces.keys())
        if len(names) >= 2:
            s, d = names[0], names[1]
            log(f"Warmup: {s} -> {d}...")
            self._send_media(s, d, content_type=3, size=10)
            time.sleep(5)

        return True

    def _discover_remotes(self):
        """Find remote device node IDs from the nodeDB of connected devices.
        Match by shortName/longName substring (their node NUMBER churns and rarely
        contains the label), and pick the most-recently-heard entry."""
        # rname -> (best_num, best_lastHeard)
        best = {rname: (None, -1) for rname in REMOTE_DEVICES}
        for name, iface in self.interfaces.items():
            try:
                nodes = iface.nodes or {}
                for node_id_str, node_info in nodes.items():
                    num = node_info.get("num", 0)
                    user = node_info.get("user", {}) or {}
                    label = f"{user.get('shortName','')} {user.get('longName','')} {num:08x}".lower()
                    lh = node_info.get("lastHeard", 0) or 0
                    for rname, rdev in REMOTE_DEVICES.items():
                        key = rdev["id_contains"].lower()
                        if key in label and lh > best[rname][1]:
                            best[rname] = (num, lh)
            except Exception as e:
                log(f"  nodeDB scan on {name} failed: {e}", "WARN")
        for rname, (num, lh) in best.items():
            if num:
                REMOTE_DEVICES[rname]["id"] = num
                self.all_devices[rname] = {"id": num, "usb": False}
                log(f"  Found remote {rname}: 0x{num:08x} (most-recent, lastHeard={lh})")
            else:
                log(f"  Remote {rname} ('{REMOTE_DEVICES[rname]['id_contains']}') NOT found in any nodeDB", "WARN")

    def _find_maluca_channel(self):
        """Find the maluca channel index from the first connected device."""
        for name, iface in self.interfaces.items():
            try:
                node = iface.getNode("^local")
                channels = node.channels
                for i, ch in enumerate(channels):
                    if ch and hasattr(ch, "settings"):
                        ch_name = ch.settings.name if ch.settings else ""
                        if ch_name.lower() == "maluca":
                            self.maluca_idx = i
                            # Compute group_id from channel hash
                            # Use CRC32 of channel name as group_id
                            self.maluca_hash = _crc32(b"maluca") & 0xFFFFFFFF
                            log(f"  Maluca channel at index {i} (group_id=0x{self.maluca_hash:08x})")
                            return
                # If not found by name, check for secondary channel
                if len(channels) > 1:
                    self.maluca_idx = 1  # fallback: channel 1 is usually maluca
                    self.maluca_hash = _crc32(b"maluca") & 0xFFFFFFFF
                    log(f"  Maluca channel assumed at index 1 (group_id=0x{self.maluca_hash:08x})")
                    return
            except Exception as e:
                log(f"  Channel scan on {name} failed: {e}", "WARN")

        self.maluca_idx = 1
        self.maluca_hash = _crc32(b"maluca") & 0xFFFFFFFF
        log(f"  Maluca channel defaulting to index 1", "WARN")

    # ─── Receive Callbacks ───────────────────────────────────────────

    def _on_rx(self, packet, interface):
        """Global receive handler for all interfaces."""
        iface_id = id(interface)
        receiver = self.iface_to_name.get(iface_id, "?")
        decoded = packet.get("decoded", {})
        portnum = decoded.get("portnum", "?")
        payload = decoded.get("payload", b"")
        rssi = packet.get("rxRssi") or packet.get("rssi")
        snr = packet.get("rxSnr") or packet.get("snr")
        hops = packet.get("hopsAway", 0)
        from_id = packet.get("fromId", "") or ""
        via_mqtt = packet.get("viaMqtt", False)

        # Text messages
        if portnum in ("TEXT_MESSAGE_APP", 1):
            text = ""
            if isinstance(payload, bytes):
                try:
                    text = payload.decode("utf-8")
                except Exception:
                    text = ""
            elif isinstance(payload, str):
                text = payload

            with self.lock:
                for tid, info in list(self.pending_rx.items()):
                    if tid in text and receiver != info.get("sender"):
                        info["received_by"].add(receiver)
                        info["rssi"] = rssi
                        info["snr"] = snr
                        info["hops_away"] = hops
                        info["path"] = "mqtt" if via_mqtt else "radio"
                        if not info["event"].is_set():
                            if info.get("expect_receiver"):
                                if receiver == info["expect_receiver"]:
                                    info["event"].set()
                            else:
                                info["event"].set()
                # DM-A: record delivery against ALL tracked tags — captures retries that
                # land after the per-message timeout (once pending_rx has been popped).
                if text:
                    for tg in self.tracked_tags:
                        if tg in text:
                            self.delivered_tags[tg].add(receiver)

        # Group messages (portnum 258)
        if portnum in (258, "GROUP_MESSAGE_APP", ""):
            # Capture GROUP_ACK (type=3): a member confirming it received a group message.
            # This is the definitive per-member delivery signal ("...have acked").
            if isinstance(payload, (bytes, bytearray)):
                gfields = _dec_fields(payload)
                if gfields.get(1) == 3:  # GROUP_ACK
                    ack_mid = gfields.get(6)        # ack_message_id
                    member = gfields.get(7)         # member_node_id
                    if ack_mid and member:
                        with self.lock:
                            self.group_acks[ack_mid].add(member)
            with self.lock:
                for tid, info in list(self.pending_rx.items()):
                    if info.get("is_group") and not info["event"].is_set():
                        # For group messages, check payload for matching text
                        if isinstance(payload, (bytes, bytearray)):
                            fields = _dec_fields(payload)
                            text_field = fields.get(4, b"")
                            if isinstance(text_field, (bytes, bytearray)):
                                try:
                                    text_field = text_field.decode("utf-8")
                                except Exception:
                                    text_field = ""
                            if tid in str(text_field) and receiver != info.get("sender"):
                                info["received_by"].add(receiver)
                                info["rssi"] = rssi
                                info["snr"] = snr
                                info["hops_away"] = hops
                                info["path"] = "mqtt" if via_mqtt else "radio"
                                info["event"].set()
                # DM-A: record group delivery against tracked tags (captures late retries)
                if isinstance(payload, (bytes, bytearray)):
                    gf = _dec_fields(payload).get(4, b"")
                    if isinstance(gf, (bytes, bytearray)):
                        try:
                            gf = gf.decode("utf-8")
                        except Exception:
                            gf = ""
                    for tg in self.tracked_tags:
                        if tg in str(gf):
                            self.delivered_tags[tg].add(receiver)

        # Media ACK/NACK (portnum 259)
        if portnum in (259, "MEDIA_TRANSFER_APP", ""):
            if isinstance(payload, (bytes, bytearray)):
                fields = _dec_fields(payload)
                mtype = fields.get(1, 0)
                mtype_str = {0: "CHUNK", 1: "START", 2: "COMPLETE", 3: "NACK",
                             4: "ACK_COMPLETE", 5: "CANCEL"}.get(mtype, f"?{mtype}")
                tid_val = fields.get(2, None)

                if mtype_str in ("ACK_COMPLETE", "NACK"):
                    with self.lock:
                        matched = False
                        if tid_val and tid_val in self.pending_ack:
                            state = self.pending_ack[tid_val]
                            if not state["event"].is_set():
                                state["result"] = mtype_str
                                state["rx_time"] = time.time()
                                state["rssi"] = rssi
                                state["snr"] = snr
                                state["hops_away"] = hops
                                state["path"] = "mqtt" if via_mqtt else "radio"
                                state["event"].set()
                                matched = True
                        if not matched:
                            for tid, state in self.pending_ack.items():
                                if not state["event"].is_set():
                                    state["result"] = mtype_str
                                    state["rx_time"] = time.time()
                                    state["rssi"] = rssi
                                    state["snr"] = snr
                                    state["hops_away"] = hops
                                    state["path"] = "mqtt" if via_mqtt else "radio"
                                    state["event"].set()
                                    break

    def _on_disconnect(self, interface):
        iface_id = id(interface)
        name = self.iface_to_name.get(iface_id)
        if name:
            log(f"{name} serial LOST", "WARN")
            self.iface_to_name.pop(iface_id, None)
            self.interfaces.pop(name, None)
            try:
                if hasattr(interface, "stream") and interface.stream:
                    interface.stream.close()
            except Exception:
                pass
            try:
                interface.close()
            except Exception:
                pass

    # ─── Reconnection ────────────────────────────────────────────────

    def _reconnect(self, name):
        if name not in USB_DEVICES:
            return False
        if name in self.skip_devices:
            return False
        # Bound reconnects so a chronically-unstable device can't stall the whole run.
        self.reconnect_count[name] += 1
        if self.reconnect_count[name] > MAX_RECONNECTS:
            log(f"  {name} dropped {self.reconnect_count[name]}x — skipping for the rest of the run", "WARN")
            self.skip_devices.add(name)
            self.interfaces.pop(name, None)
            return False
        log(f"Reconnecting {name} (#{self.reconnect_count[name]})...")
        old = self.interfaces.pop(name, None)
        if old:
            self.iface_to_name.pop(id(old), None)
            run_with_timeout(lambda: old.close(), 6)  # guarded — a wedged port can hang close()
        time.sleep(2)
        # Re-discover the device's CURRENT port by identity — it may have re-enumerated
        # to a different port when it dropped (the old hardcoded port is stale). Every
        # open/probe is timeout-guarded because a WEDGED tbeam serial port hangs forever.
        import glob as _glob
        want = NAME_TO_LONGNAME.get(name, "").strip().lower()
        held = {USB_DEVICES.get(n, {}).get("port") for n in self.interfaces}
        def _probe(p):
            ifc = meshtastic.serial_interface.SerialInterface(p)
            time.sleep(3)
            return ifc, ifc.getMyNodeInfo()
        for port in sorted(_glob.glob("/dev/cu.usbmodem*")):
            if port in held:
                continue
            res, ok = run_with_timeout(lambda: _probe(port), 30)
            if not ok or not res:
                continue  # hung/failed open — abandon this port
            iface, node = res
            if not node:  # getMyNodeInfo() returned None — handshake not complete; skip port
                run_with_timeout(lambda: iface.close(), 4); continue
            ln = ((node.get("user") or {}).get("longName") or "").strip().lower()
            if want and ln != want:
                run_with_timeout(lambda: iface.close(), 4); continue
            num = node.get("num", 0)
            USB_DEVICES[name]["port"] = port
            USB_DEVICES[name]["id"] = num
            guard_iface_sends(iface, name)
            self.interfaces[name] = iface
            self.iface_to_name[id(iface)] = name
            log(f"  {name} reconnected on {port}")
            return True
        log(f"  {name} not found on any free port (will retry up to {MAX_RECONNECTS}x)", "WARN")
        return False

    def _ensure_connected(self, name):
        if name not in USB_DEVICES:
            return name in self.all_devices  # remote device
        if name not in self.interfaces:
            return self._reconnect(name)
        iface = self.interfaces[name]
        try:
            if hasattr(iface, "isConnected") and not iface.isConnected.is_set():
                return self._reconnect(name)
            if hasattr(iface, "stream") and hasattr(iface.stream, "is_open") and not iface.stream.is_open:
                return self._reconnect(name)
            return True
        except Exception:
            return self._reconnect(name)

    def _should_skip(self, name):
        return name in self.skip_devices

    def _record_failure(self, name):
        self.consec_fail[name] += 1
        if self.consec_fail[name] >= 3:
            self.skip_devices.add(name)
            log(f"{name}: 3+ consecutive failures — SKIPPING for this phase", "WARN")

    def _record_success(self, name):
        self.consec_fail[name] = 0

    def _reset_phase(self):
        """Reset skip list and reconnect failed devices between phases."""
        if self.skip_devices:
            log(f"Resetting skipped devices: {', '.join(self.skip_devices)}")
        self.skip_devices.clear()
        self.consec_fail.clear()
        self.reconnect_count.clear()  # fresh reconnect budget each phase
        # Try reconnecting any missing USB devices
        for name in USB_DEVICES:
            if name not in self.interfaces:
                self._reconnect(name)

    # ─── Get destination ID ──────────────────────────────────────────

    def _get_dest_id(self, name):
        if name in USB_DEVICES:
            return USB_DEVICES[name]["id"]
        if name in REMOTE_DEVICES:
            return REMOTE_DEVICES[name]["id"]
        return None

    def _get_all_ids(self):
        """Return list of all known node IDs. If GROUP_MEMBERS env is set (comma-separated
        device names, e.g. 'T3S3,XIAO,Pager'), restrict the group roster to just those —
        used to test the group protocol among reliable same-band members."""
        restrict = os.environ.get("GROUP_MEMBERS")
        allow = {n.strip() for n in restrict.split(",")} if restrict else None
        ids = []
        for name in USB_DEVICES:
            if allow is not None and name not in allow:
                continue
            nid = USB_DEVICES[name]["id"]
            if nid:
                ids.append(nid)
        for name in REMOTE_DEVICES:
            if allow is not None and name not in allow:
                continue
            nid = REMOTE_DEVICES[name]["id"]
            if nid:
                ids.append(nid)
        return ids

    # ─── Text DM ─────────────────────────────────────────────────────

    def _send_text_dm(self, src, dst, phase="text_dm"):
        """Send text DM, verify receipt. Returns result dict."""
        iface = self.interfaces.get(src)
        dest_id = self._get_dest_id(dst)
        if not iface or not dest_id:
            return self._fail_result(phase, src, dst, "no_interface_or_id")

        tid = f"TXT-{src[:2]}{dst[:2]}-{int(time.time())%100000}-{random.randint(100,999)}"
        msg = f"{tid} txt"

        event = threading.Event()
        with self.lock:
            self.pending_rx[tid] = {
                "event": event, "sender": src,
                "expect_receiver": dst if dst in self.interfaces else None,
                "received_by": set(), "rssi": None, "snr": None,
                "hops_away": 0, "path": "radio", "is_group": False,
            }

        send_time = time.time()
        try:
            iface.sendText(msg, destinationId=dest_id, wantAck=True, wantResponse=False)
        except Exception as e:
            with self.lock:
                self.pending_rx.pop(tid, None)
            self._reconnect(src)
            return self._fail_result(phase, src, dst, f"send_error: {e}")

        got = event.wait(timeout=DM_TIMEOUT)
        with self.lock:
            info = self.pending_rx.pop(tid, {})

        result = {
            "phase": phase, "msg_id": tid, "src": src, "dst": dst,
            "send_time": send_time, "ack_time": None, "receive_time": None,
            "ios_receive_time": None, "latency_s": None, "success": False,
            "hops_away": info.get("hops_away", 0),
            "rssi": info.get("rssi"), "snr": info.get("snr"),
            "path": info.get("path", "radio"), "error": None,
        }

        # Check serial receipt
        if got and (dst in info.get("received_by", set()) or
                    (dst not in self.interfaces and len(info.get("received_by", set())) > 0)):
            result["success"] = True
            result["receive_time"] = time.time()
            result["latency_s"] = time.time() - send_time

        # Check iOS for messages involving Pager
        if src == "Pager" or dst == "Pager":
            ios_msg = self.ios.check_message(tid, timeout=3)
            if ios_msg:
                result["ios_receive_time"] = time.time()
                if not result["success"]:
                    result["success"] = True  # iOS confirmed delivery
                    if result["latency_s"] is None:
                        result["latency_s"] = time.time() - send_time
                    result["path"] = result.get("path") or "mqtt/ble"

        if not result["success"]:
            result["error"] = "no_receipt"

        return result

    # ─── Media Transfer (Voice / Image) ──────────────────────────────

    def _send_media(self, src, dst, content_type, size, phase="media"):
        """Send media transfer. content_type: 0=voice, 1=image, 3=warmup.
        Returns (transfer_id, success, latency)."""
        iface = self.interfaces.get(src)
        dest_id = self._get_dest_id(dst)
        if not iface or not dest_id:
            return None, False, None

        # Generate synthetic data
        data = bytes([(i * 37 + random.randint(0, 255)) & 0xFF for i in range(size)])
        checksum = _crc32(data)
        tid = random.randint(0x10000, 0xFFFFFFF)

        # Build packets
        start_pkt = bytearray()
        start_pkt.extend(_fv(1, 1))         # type=START
        start_pkt.extend(_fv(2, tid))
        start_pkt.extend(_fv(4, 1))         # totalChunks=1
        start_pkt.extend(_fv(5, size))
        start_pkt.extend(_fv(7, content_type))
        start_pkt.extend(_fv(9, checksum))
        if content_type == 0:  # voice
            start_pkt.extend(_fb(10, "audio/codec2"))
            start_pkt.extend(_fv(11, 3))    # duration_seconds
        elif content_type == 1:  # image
            start_pkt.extend(_fb(10, "image/jpeg"))
            start_pkt.extend(_fv(12, 32))   # width
            start_pkt.extend(_fv(13, 32))   # height

        chunk_pkt = bytearray()
        chunk_pkt.extend(_fv(2, tid))
        chunk_pkt.extend(_fb(6, data))

        complete_pkt = bytearray()
        complete_pkt.extend(_fv(1, 2))       # type=COMPLETE
        complete_pkt.extend(_fv(2, tid))
        complete_pkt.extend(_fv(9, checksum))

        ack_event = threading.Event()
        with self.lock:
            self.pending_ack[tid] = {
                "event": ack_event, "result": None,
                "time": time.time(), "rx_time": None,
                "rssi": None, "snr": None, "hops_away": 0, "path": "radio",
            }

        # RELIABLE DELIVERY (lesson from DMs/group): the harness injects raw media packets,
        # so the firmware sender's NACK-retransmit never runs — we must retransmit ourselves.
        # Resend the whole transfer until ACK_COMPLETE arrives, up to MEDIA_MAX_ATTEMPTS, each
        # attempt waiting MEDIA_ATTEMPT_TIMEOUT. (A lost chunk otherwise = permanent fail.)
        # WEDGE-RESILIENT: a send that raises (USB-CDC wedged mid-transfer) must NOT abort the
        # transfer — reconnect and retry the attempt. Media-over-serial wedges the CDC, but the
        # device recovers; keep retrying until ACK_COMPLETE so the wedge doesn't cause a loss.
        for attempt in range(1, MEDIA_MAX_ATTEMPTS + 1):
            try:
                iface = self.interfaces.get(src)
                if not iface:
                    raise RuntimeError("no iface")
                iface.sendData(bytes(start_pkt), destinationId=dest_id,
                               portNum=PORTNUM_MEDIA, wantAck=False, wantResponse=False)
                time.sleep(MEDIA_CHUNK_DELAY)
                iface.sendData(bytes(chunk_pkt), destinationId=dest_id,
                               portNum=PORTNUM_MEDIA, wantAck=False, wantResponse=False)
                time.sleep(MEDIA_CHUNK_DELAY)
                iface.sendData(bytes(complete_pkt), destinationId=dest_id,
                               portNum=PORTNUM_MEDIA, wantAck=False, wantResponse=False)
                if ack_event.wait(timeout=MEDIA_ATTEMPT_TIMEOUT):
                    break  # ACK_COMPLETE received
            except Exception:
                pass  # wedged mid-send — fall through to reconnect + retry
            if attempt < MEDIA_MAX_ATTEMPTS and not self.pending_ack.get(tid, {}).get("result"):
                self._ensure_connected(src)  # recover a wedged device, then retry the transfer

        with self.lock:
            state = self.pending_ack.pop(tid, None)

        if state and state["result"] == "ACK_COMPLETE":
            lat = (state["rx_time"] or time.time()) - state["time"]
            return tid, True, lat
        return tid, False, None

    def _send_cancel(self, src, dst, tid):
        """Send CANCEL packet twice to clear stale state."""
        if not tid:
            return
        iface = self.interfaces.get(src)
        dest_id = self._get_dest_id(dst)
        if not iface or not dest_id:
            return
        cancel_pkt = bytearray()
        cancel_pkt.extend(_fv(1, 5))  # type=CANCEL
        cancel_pkt.extend(_fv(2, tid))
        try:
            iface.sendData(bytes(cancel_pkt), destinationId=dest_id,
                           portNum=PORTNUM_MEDIA, wantAck=False, wantResponse=False)
            time.sleep(3)
            iface.sendData(bytes(cancel_pkt), destinationId=dest_id,
                           portNum=PORTNUM_MEDIA, wantAck=False, wantResponse=False)
        except Exception:
            pass
        time.sleep(3)

    def _send_voice_dm(self, src, dst):
        """Send voice memo DM with retry."""
        size = random.choice([50, 100, 200, 300])
        tid, ok, lat = self._send_media(src, dst, content_type=0, size=size, phase="voice_dm")
        if not ok:
            self._send_cancel(src, dst, tid)
            time.sleep(2)
            tid, ok, lat = self._send_media(src, dst, content_type=0, size=random.choice([50, 100]),
                                             phase="voice_dm")
        return tid, ok, lat

    def _send_image_dm(self, src, dst):
        """Send image DM with retry."""
        size = random.choice([50, 100, 200, 400])
        tid, ok, lat = self._send_media(src, dst, content_type=1, size=size, phase="image_dm")
        if not ok:
            self._send_cancel(src, dst, tid)
            time.sleep(2)
            tid, ok, lat = self._send_media(src, dst, content_type=1, size=random.choice([50, 100]),
                                             phase="image_dm")
        return tid, ok, lat

    # ─── Group Messages ──────────────────────────────────────────────

    def _online_member_ids(self, members):
        """Subset of `members` (node IDs) online in the last 10 min. USB-connected devices
        are always online; remotes are judged by the most-recent nodeDB lastHeard."""
        usb_ids = {d["id"] for d in self.all_devices.values() if d.get("usb") and d.get("id")}
        now = time.time()
        heard = {}
        for iface in list(self.interfaces.values()):
            try:
                for _, ninfo in (iface.nodes or {}).items():
                    num = ninfo.get("num", 0)
                    lh = ninfo.get("lastHeard", 0) or 0
                    if num:
                        heard[num] = max(heard.get(num, 0), lh)
            except Exception:
                pass
        online = set()
        for m in members:
            if m in usb_ids or (now - heard.get(m, 0) <= 600):
                online.add(m)
        return online

    def _group_success(self, tag, info=None):
        """A group message is delivered when every member online in the last 10 min (except
        the sender) has RECEIVED it. We count two independent signals, whichever arrives:
          1. the member's GROUP_ACK reached the sender (group_acks), AND
          2. the member's own _on_rx actually showed the text (delivered_tags) — the direct
             delivery observation, which survives even if the ACK round-trip was lost.
        Using actual reception (not just the lossy ACK) avoids undercounting real deliveries."""
        meta = self.group_msg_meta.get(tag)
        if not meta:
            return False
        online = set(meta.get("online", set())) - {meta.get("src_id")}
        if not online:
            return True  # no other online members to deliver to
        nm2id = {n: d.get("id") for n, d in self.all_devices.items()}
        delivered = set(self.group_acks.get(meta["msg_id"], set()))
        # actual reception observed at each member (persistent across the whole run)
        for rn in self.delivered_tags.get(tag, set()):
            if nm2id.get(rn):
                delivered.add(nm2id[rn])
        if info:
            for rn in info.get("received_by", set()):
                if nm2id.get(rn):
                    delivered.add(nm2id[rn])
        return online.issubset(delivered)

    def _send_group_text(self, src, tracking_tag):
        """Send GROUP_TEXT on portnum 258, broadcast on maluca channel."""
        iface = self.interfaces.get(src)
        if not iface:
            return False

        members = self._get_all_ids()
        msg_id = random.randint(1, 0xFFFFFFFF)
        text = f"{tracking_tag} grptxt"
        self.group_msg_meta[tracking_tag] = {
            "msg_id": msg_id,
            "src_id": self.all_devices.get(src, {}).get("id"),
            "online": self._online_member_ids(members),
        }

        payload = bytearray()
        payload.extend(_fv_force(1, 0))        # type=GROUP_TEXT (0)
        payload.extend(_fv(2, msg_id))
        payload.extend(_fv(3, self.maluca_hash or 0))
        payload.extend(_fb(4, text))
        # Encode members as individual varints (not packed) for firmware compat
        for m in members:
            payload.extend(_fv(5, m))
        payload.extend(_fv(9, int(time.time())))

        event = threading.Event()
        with self.lock:
            self.pending_rx[tracking_tag] = {
                "event": event, "sender": src, "expect_receiver": None,
                "received_by": set(), "rssi": None, "snr": None,
                "hops_away": 0, "path": "radio", "is_group": True,
            }

        try:
            ch_idx = self.maluca_idx or 0
            iface.sendData(bytes(payload), destinationId="^all",
                           portNum=PORTNUM_GROUP, channelIndex=ch_idx,
                           wantAck=False, wantResponse=False)
        except Exception as e:
            with self.lock:
                self.pending_rx.pop(tracking_tag, None)
            self._reconnect(src)
            return False

        event.wait(timeout=GROUP_MSG_WAIT)
        with self.lock:
            info = self.pending_rx.pop(tracking_tag, {})
        # Success = every member online in the last 10 min (except the sender) has ACKed
        # (or its USB _on_rx saw the text). The 5-min reconciliation re-checks as the
        # hybrid reliable-unicast retries land more ACKs.
        return self._group_success(tracking_tag, info)

    def _send_group_voice(self, src, tracking_tag):
        """Send voice memo as broadcast to group via media transfer."""
        iface = self.interfaces.get(src)
        if not iface:
            return False

        size = random.choice([50, 100])
        data = bytes([(i * 37 + random.randint(0, 255)) & 0xFF for i in range(size)])
        checksum = _crc32(data)
        tid = random.randint(0x10000, 0xFFFFFFF)

        start_pkt = bytearray()
        start_pkt.extend(_fv(1, 1))
        start_pkt.extend(_fv(2, tid))
        start_pkt.extend(_fv(4, 1))
        start_pkt.extend(_fv(5, size))
        start_pkt.extend(_fv(7, 0))  # VOICE_MEMO
        start_pkt.extend(_fv(9, checksum))
        start_pkt.extend(_fb(10, "audio/codec2"))
        start_pkt.extend(_fv(11, 3))

        chunk_pkt = bytearray()
        chunk_pkt.extend(_fv(2, tid))
        chunk_pkt.extend(_fb(6, data))

        complete_pkt = bytearray()
        complete_pkt.extend(_fv(1, 2))
        complete_pkt.extend(_fv(2, tid))
        complete_pkt.extend(_fv(9, checksum))

        ack_event = threading.Event()
        with self.lock:
            self.pending_ack[tid] = {
                "event": ack_event, "result": None,
                "time": time.time(), "rx_time": None,
                "rssi": None, "snr": None, "hops_away": 0, "path": "radio",
            }

        try:
            ch_idx = self.maluca_idx or 0
            iface.sendData(bytes(start_pkt), destinationId="^all",
                           portNum=PORTNUM_MEDIA, channelIndex=ch_idx,
                           wantAck=False, wantResponse=False)
            time.sleep(MEDIA_CHUNK_DELAY)
            iface.sendData(bytes(chunk_pkt), destinationId="^all",
                           portNum=PORTNUM_MEDIA, channelIndex=ch_idx,
                           wantAck=False, wantResponse=False)
            time.sleep(MEDIA_CHUNK_DELAY)
            iface.sendData(bytes(complete_pkt), destinationId="^all",
                           portNum=PORTNUM_MEDIA, channelIndex=ch_idx,
                           wantAck=False, wantResponse=False)
        except Exception as e:
            with self.lock:
                self.pending_ack.pop(tid, None)
            return False

        got = ack_event.wait(timeout=MEDIA_ACK_TIMEOUT)
        with self.lock:
            state = self.pending_ack.pop(tid, None)

        return state and state["result"] == "ACK_COMPLETE"

    def _send_group_image(self, src, tracking_tag):
        """Send image as broadcast to group via media transfer."""
        iface = self.interfaces.get(src)
        if not iface:
            return False

        size = random.choice([50, 100])
        # Fake JPEG header
        data = bytearray(b"\xFF\xD8\xFF\xE0")
        data.extend(bytes([(i * 41) & 0xFF for i in range(size - 4)]))
        data = bytes(data)
        checksum = _crc32(data)
        tid = random.randint(0x10000, 0xFFFFFFF)

        start_pkt = bytearray()
        start_pkt.extend(_fv(1, 1))
        start_pkt.extend(_fv(2, tid))
        start_pkt.extend(_fv(4, 1))
        start_pkt.extend(_fv(5, size))
        start_pkt.extend(_fv(7, 1))  # IMAGE_THUMBNAIL
        start_pkt.extend(_fv(9, checksum))
        start_pkt.extend(_fb(10, "image/jpeg"))
        start_pkt.extend(_fv(12, 32))
        start_pkt.extend(_fv(13, 32))

        chunk_pkt = bytearray()
        chunk_pkt.extend(_fv(2, tid))
        chunk_pkt.extend(_fb(6, data))

        complete_pkt = bytearray()
        complete_pkt.extend(_fv(1, 2))
        complete_pkt.extend(_fv(2, tid))
        complete_pkt.extend(_fv(9, checksum))

        ack_event = threading.Event()
        with self.lock:
            self.pending_ack[tid] = {
                "event": ack_event, "result": None,
                "time": time.time(), "rx_time": None,
                "rssi": None, "snr": None, "hops_away": 0, "path": "radio",
            }

        try:
            ch_idx = self.maluca_idx or 0
            iface.sendData(bytes(start_pkt), destinationId="^all",
                           portNum=PORTNUM_MEDIA, channelIndex=ch_idx,
                           wantAck=False, wantResponse=False)
            time.sleep(MEDIA_CHUNK_DELAY)
            iface.sendData(bytes(chunk_pkt), destinationId="^all",
                           portNum=PORTNUM_MEDIA, channelIndex=ch_idx,
                           wantAck=False, wantResponse=False)
            time.sleep(MEDIA_CHUNK_DELAY)
            iface.sendData(bytes(complete_pkt), destinationId="^all",
                           portNum=PORTNUM_MEDIA, channelIndex=ch_idx,
                           wantAck=False, wantResponse=False)
        except Exception as e:
            with self.lock:
                self.pending_ack.pop(tid, None)
            return False

        got = ack_event.wait(timeout=MEDIA_ACK_TIMEOUT)
        with self.lock:
            state = self.pending_ack.pop(tid, None)

        return state and state["result"] == "ACK_COMPLETE"

    # ─── Channel Broadcast ───────────────────────────────────────────

    def _send_channel_broadcast(self, src, tracking_tag):
        """Send broadcast on maluca channel, verify receipt on other USB devices."""
        iface = self.interfaces.get(src)
        if not iface:
            return False, set()

        msg = f"{tracking_tag} bcast"
        event = threading.Event()
        with self.lock:
            self.pending_rx[tracking_tag] = {
                "event": event, "sender": src, "expect_receiver": None,
                "received_by": set(), "rssi": None, "snr": None,
                "hops_away": 0, "path": "radio", "is_group": False,
            }

        try:
            ch_idx = self.maluca_idx or 0
            iface.sendText(msg, channelIndex=ch_idx, wantAck=False, wantResponse=False)
        except Exception as e:
            with self.lock:
                self.pending_rx.pop(tracking_tag, None)
            self._reconnect(src)
            return False, set()

        got = event.wait(timeout=BROADCAST_TIMEOUT)
        with self.lock:
            info = self.pending_rx.pop(tracking_tag, {})

        receivers = info.get("received_by", set()) - {src}
        # Also count iOS-app reception: the Pager frequently receives the broadcast
        # over BLE even when its USB-serial _on_rx is disconnected during the run, so
        # USB-only verification under-counts real delivery.
        if not receivers and self.ios.available and self.ios.check_message(tracking_tag, timeout=4):
            return True, {"iOS"}
        return len(receivers) > 0, receivers

    # ─── Result Helpers ──────────────────────────────────────────────

    def _fail_result(self, phase, src, dst, error):
        return {
            "phase": phase, "msg_id": None, "src": src, "dst": dst,
            "send_time": time.time(), "ack_time": None, "receive_time": None,
            "ios_receive_time": None, "latency_s": None, "success": False,
            "hops_away": 0, "rssi": None, "snr": None,
            "path": None, "error": error,
        }

    def _log_result(self, result):
        """Append result to JSONL and results list."""
        self.results.append(result)
        if result.get("msg_id"):
            self.tracked_tags.add(result["msg_id"])  # DM-A: track for post-phase retry reconciliation
        phase = result["phase"]
        self.phase_stats[phase]["sent"] += 1
        if result["success"]:
            self.phase_stats[phase]["ok"] += 1
        else:
            self.phase_stats[phase]["fail"] += 1

        try:
            with open(JSONL_FILE, "a") as f:
                f.write(json.dumps(result, default=str) + "\n")
        except Exception:
            pass

    def _reconcile_phase(self, phase):
        """DM-A: after a phase, wait a grace window for the firmware's retries to deliver,
        then upgrade any message scored FAIL that actually got received (delivered_tags)
        or that the iOS app shows. Reliability matters more than the short send-window, so
        we measure eventual delivery, not just first-attempt."""
        failed = [r for r in self.results if r["phase"] == phase and not r["success"]]
        if not failed:
            return
        log(f"  [DM-A] reconciling {len(failed)} unconfirmed {phase} msg(s), waiting up to "
            f"{RETRY_RECONCILE_GRACE}s for retries...")
        deadline = time.time() + RETRY_RECONCILE_GRACE
        while time.time() < deadline:
            pending = [r for r in failed if not r["success"]]
            if not pending:
                break
            for r in pending:
                tag = r.get("msg_id")
                if not tag:
                    continue
                if phase.startswith("group_"):
                    # Group success = all online members ACKed (hybrid reliable-unicast retries
                    # keep landing ACKs over the 5-min window).
                    got = self._group_success(tag)
                else:
                    got = len(self.delivered_tags.get(tag, ())) > 0
                if not got and self.ios.available and self.ios.check_message(tag, timeout=2):
                    got = True
                if got:
                    r["success"] = True
                    r["error"] = "delivered_via_retry"
                    r["receive_time"] = time.time()
                    self.phase_stats[phase]["ok"] += 1
                    self.phase_stats[phase]["fail"] -= 1
                    log(f"    ↑ {phase}: {r['src']}->{r.get('dst')} delivered via retry")
            time.sleep(8)
        up = sum(1 for r in failed if r["success"])
        log(f"  [DM-A] {up}/{len(failed)} late deliveries; {phase} now "
            f"{self.phase_stats[phase]['ok']}/{self.phase_stats[phase]['sent']}")

        # Per-member diagnostic for group phases: of the messages where member M was an
        # expected (online) recipient, how many did M ACK? Pinpoints the bottleneck member.
        if phase.startswith("group_"):
            id2name = {d.get("id"): n for n, d in self.all_devices.items()}
            expected = collections.Counter()
            acked = collections.Counter()
            for r in self.results:
                if r["phase"] != phase:
                    continue
                meta = self.group_msg_meta.get(r.get("msg_id"))
                if not meta:
                    continue
                online = set(meta.get("online", set())) - {meta.get("src_id")}
                got = self.group_acks.get(meta["msg_id"], set())
                for m in online:
                    expected[m] += 1
                    if m in got:
                        acked[m] += 1
            log("  [GRP] per-member ACK rate (acked / expected-as-recipient):")
            for m in sorted(expected):
                nm = id2name.get(m, f"0x{m:08x}")
                log(f"        {nm:14} 0x{m:08x}  {acked[m]}/{expected[m]}")

    # ─── Round-Robin Target Selection ────────────────────────────────

    def _get_targets_for(self, src, count, usb_only=True):
        """Get round-robin targets for src device.
        usb_only=True excludes remote devices (can't verify DM receipt via serial)."""
        if usb_only:
            candidates = [n for n in USB_DEVICES
                          if n != src and n in self.interfaces and not self._should_skip(n)]
        else:
            candidates = [n for n in ALL_DEVICE_NAMES
                          if n != src and not self._should_skip(n) and self._get_dest_id(n)]
        if not candidates:
            return []
        targets = []
        for i in range(count):
            targets.append(candidates[i % len(candidates)])
        return targets

    # ─── Phase Runners ───────────────────────────────────────────────

    def run_phase_text_dm(self):
        """Phase 2: Text DMs — each USB device sends 5 DMs to round-robin targets."""
        log(f"\n{'='*70}")
        log(f"  PHASE 2: TEXT DMs ({MSGS_PER_PHASE} messages)")
        log(f"{'='*70}")

        active = [n for n in USB_DEVICES if n in self.interfaces and not self._should_skip(n)]
        msgs_per_device = MSGS_PER_PHASE // len(active) if active else 0
        extra = MSGS_PER_PHASE - msgs_per_device * len(active)
        sent = 0

        for idx, src in enumerate(active):
            count = msgs_per_device + (1 if idx < extra else 0)
            targets = self._get_targets_for(src, count)
            for i, dst in enumerate(targets):
                if not self._ensure_connected(src):
                    self._log_result(self._fail_result("text_dm", src, dst, "disconnected"))
                    continue

                sent += 1
                log(f"  [{sent}/{MSGS_PER_PHASE}] TEXT_DM: {src} -> {dst}", end="")
                result = self._send_text_dm(src, dst, phase="text_dm")
                self._log_result(result)

                status = f" {C['g']}OK{C['0']} {(result['latency_s'] or 0):.1f}s" if result["success"] \
                    else f" {C['r']}FAIL{C['0']}"
                print(status, flush=True)

                if result["success"]:
                    self._record_success(src)
                else:
                    self._record_failure(src)

                time.sleep(INTER_MSG_DELAY)

    def run_phase_voice_dm(self):
        """Phase 3: Voice DMs — media transfer with content_type=0."""
        log(f"\n{'='*70}")
        log(f"  PHASE 3: VOICE DMs ({MSGS_PER_PHASE} messages)")
        log(f"{'='*70}")

        active = [n for n in USB_DEVICES if n in self.interfaces and not self._should_skip(n)]
        msgs_per_device = MSGS_PER_PHASE // len(active) if active else 0
        extra = MSGS_PER_PHASE - msgs_per_device * len(active)
        sent = 0

        for idx, src in enumerate(active):
            count = msgs_per_device + (1 if idx < extra else 0)
            targets = self._get_targets_for(src, count)
            for i, dst in enumerate(targets):
                if not self._ensure_connected(src):
                    self._log_result(self._fail_result("voice_dm", src, dst, "disconnected"))
                    continue

                sent += 1
                tid_tag = f"VOC-{src[:2]}{dst[:2]}-{int(time.time())%100000}-{random.randint(100,999)}"
                log(f"  [{sent}/{MSGS_PER_PHASE}] VOICE_DM: {src} -> {dst}", end="")

                send_time = time.time()
                tid, ok, lat = self._send_voice_dm(src, dst)

                result = {
                    "phase": "voice_dm", "msg_id": tid_tag, "src": src, "dst": dst,
                    "send_time": send_time, "ack_time": None, "receive_time": None,
                    "ios_receive_time": None, "latency_s": lat, "success": ok,
                    "hops_away": 0, "rssi": None, "snr": None,
                    "path": "radio", "error": None if ok else "no_ack",
                }
                self._log_result(result)

                status = f" {C['g']}OK{C['0']} {(lat or 0):.1f}s" if ok else f" {C['r']}FAIL{C['0']}"
                print(status, flush=True)

                if ok:
                    self._record_success(src)
                else:
                    self._record_failure(src)

                time.sleep(INTER_MSG_DELAY + MEDIA_SEND_DELAY)

    def run_phase_image_dm(self):
        """Phase 4: Image DMs — media transfer with content_type=1."""
        log(f"\n{'='*70}")
        log(f"  PHASE 4: IMAGE DMs ({MSGS_PER_PHASE} messages)")
        log(f"{'='*70}")

        active = [n for n in USB_DEVICES if n in self.interfaces and not self._should_skip(n)]
        msgs_per_device = MSGS_PER_PHASE // len(active) if active else 0
        extra = MSGS_PER_PHASE - msgs_per_device * len(active)
        sent = 0

        for idx, src in enumerate(active):
            count = msgs_per_device + (1 if idx < extra else 0)
            targets = self._get_targets_for(src, count)
            for i, dst in enumerate(targets):
                if not self._ensure_connected(src):
                    self._log_result(self._fail_result("image_dm", src, dst, "disconnected"))
                    continue

                sent += 1
                tid_tag = f"IMG-{src[:2]}{dst[:2]}-{int(time.time())%100000}-{random.randint(100,999)}"
                log(f"  [{sent}/{MSGS_PER_PHASE}] IMAGE_DM: {src} -> {dst}", end="")

                send_time = time.time()
                tid, ok, lat = self._send_image_dm(src, dst)

                result = {
                    "phase": "image_dm", "msg_id": tid_tag, "src": src, "dst": dst,
                    "send_time": send_time, "ack_time": None, "receive_time": None,
                    "ios_receive_time": None, "latency_s": lat, "success": ok,
                    "hops_away": 0, "rssi": None, "snr": None,
                    "path": "radio", "error": None if ok else "no_ack",
                }
                self._log_result(result)

                status = f" {C['g']}OK{C['0']} {(lat or 0):.1f}s" if ok else f" {C['r']}FAIL{C['0']}"
                print(status, flush=True)

                if ok:
                    self._record_success(src)
                else:
                    self._record_failure(src)

                time.sleep(INTER_MSG_DELAY + MEDIA_SEND_DELAY)

    def run_phase_group_text(self):
        """Phase 6: Group text messages on portnum 258."""
        log(f"\n{'='*70}")
        log(f"  PHASE 6: GROUP TEXT ({MSGS_PER_PHASE} messages)")
        log(f"{'='*70}")

        active = [n for n in USB_DEVICES if n in self.interfaces and not self._should_skip(n)]
        sent = 0

        for i in range(MSGS_PER_PHASE):
            src = active[i % len(active)]
            if not self._ensure_connected(src):
                self._log_result(self._fail_result("group_text", src, "ALL", "disconnected"))
                continue

            sent += 1
            tag = f"GRP-{src[:2]}-{int(time.time())%100000}-{random.randint(100,999)}"
            log(f"  [{sent}/{MSGS_PER_PHASE}] GROUP_TEXT: {src} -> ALL", end="")

            send_time = time.time()
            ok = self._send_group_text(src, tag)

            result = {
                "phase": "group_text", "msg_id": tag, "src": src, "dst": "ALL",
                "send_time": send_time, "ack_time": None, "receive_time": None,
                "ios_receive_time": None, "latency_s": time.time() - send_time if ok else None,
                "success": ok, "hops_away": 0, "rssi": None, "snr": None,
                "path": "radio", "error": None if ok else "no_receipt",
            }

            # Check iOS
            if src == "Pager":
                ios_msg = self.ios.check_message(tag, timeout=3)
                if ios_msg:
                    result["ios_receive_time"] = time.time()

            self._log_result(result)

            status = f" {C['g']}OK{C['0']}" if ok else f" {C['r']}FAIL{C['0']}"
            print(status, flush=True)

            if ok:
                self._record_success(src)
            else:
                self._record_failure(src)

            time.sleep(int(os.environ.get("GROUP_SEND_DELAY", INTER_MSG_DELAY)))

    def run_phase_group_voice(self):
        """Phase 7: Group voice memos via media transfer broadcast."""
        log(f"\n{'='*70}")
        log(f"  PHASE 7: GROUP VOICE ({MSGS_PER_PHASE} messages)")
        log(f"{'='*70}")

        active = [n for n in USB_DEVICES if n in self.interfaces and not self._should_skip(n)]
        sent = 0

        for i in range(MSGS_PER_PHASE):
            src = active[i % len(active)]
            if not self._ensure_connected(src):
                self._log_result(self._fail_result("group_voice", src, "ALL", "disconnected"))
                continue

            sent += 1
            tag = f"GRV-{src[:2]}-{int(time.time())%100000}-{random.randint(100,999)}"
            log(f"  [{sent}/{MSGS_PER_PHASE}] GROUP_VOICE: {src} -> ALL", end="")

            send_time = time.time()
            ok = self._send_group_voice(src, tag)

            result = {
                "phase": "group_voice", "msg_id": tag, "src": src, "dst": "ALL",
                "send_time": send_time, "ack_time": None, "receive_time": None,
                "ios_receive_time": None, "latency_s": time.time() - send_time if ok else None,
                "success": ok, "hops_away": 0, "rssi": None, "snr": None,
                "path": "radio", "error": None if ok else "no_ack",
            }
            self._log_result(result)

            status = f" {C['g']}OK{C['0']}" if ok else f" {C['r']}FAIL{C['0']}"
            print(status, flush=True)

            if ok:
                self._record_success(src)
            else:
                self._record_failure(src)

            time.sleep(INTER_MSG_DELAY)

    def run_phase_group_image(self):
        """Phase 8: Group image transfers via media transfer broadcast."""
        log(f"\n{'='*70}")
        log(f"  PHASE 8: GROUP IMAGE ({MSGS_PER_PHASE} messages)")
        log(f"{'='*70}")

        active = [n for n in USB_DEVICES if n in self.interfaces and not self._should_skip(n)]
        sent = 0

        for i in range(MSGS_PER_PHASE):
            src = active[i % len(active)]
            if not self._ensure_connected(src):
                self._log_result(self._fail_result("group_image", src, "ALL", "disconnected"))
                continue

            sent += 1
            tag = f"GRI-{src[:2]}-{int(time.time())%100000}-{random.randint(100,999)}"
            log(f"  [{sent}/{MSGS_PER_PHASE}] GROUP_IMAGE: {src} -> ALL", end="")

            send_time = time.time()
            ok = self._send_group_image(src, tag)

            result = {
                "phase": "group_image", "msg_id": tag, "src": src, "dst": "ALL",
                "send_time": send_time, "ack_time": None, "receive_time": None,
                "ios_receive_time": None, "latency_s": time.time() - send_time if ok else None,
                "success": ok, "hops_away": 0, "rssi": None, "snr": None,
                "path": "radio", "error": None if ok else "no_ack",
            }
            self._log_result(result)

            status = f" {C['g']}OK{C['0']}" if ok else f" {C['r']}FAIL{C['0']}"
            print(status, flush=True)

            if ok:
                self._record_success(src)
            else:
                self._record_failure(src)

            time.sleep(INTER_MSG_DELAY)

    def run_phase_channel_broadcast(self):
        """Phase 9: Channel broadcasts on maluca channel."""
        log(f"\n{'='*70}")
        log(f"  PHASE 9: CHANNEL BROADCASTS ({MSGS_PER_PHASE} messages)")
        log(f"{'='*70}")

        active = [n for n in USB_DEVICES if n in self.interfaces and not self._should_skip(n)]
        sent = 0

        for i in range(MSGS_PER_PHASE):
            src = active[i % len(active)]
            if not self._ensure_connected(src):
                self._log_result(self._fail_result("channel_broadcast", src, "ALL", "disconnected"))
                continue

            sent += 1
            tag = f"CH-{src[:2]}-{int(time.time())%100000}-{random.randint(100,999)}"
            log(f"  [{sent}/{MSGS_PER_PHASE}] CHANNEL: {src} -> ALL", end="")

            send_time = time.time()
            ok, receivers = self._send_channel_broadcast(src, tag)

            result = {
                "phase": "channel_broadcast", "msg_id": tag, "src": src, "dst": "ALL",
                "send_time": send_time, "ack_time": None, "receive_time": None,
                "ios_receive_time": None,
                "latency_s": time.time() - send_time if ok else None,
                "success": ok, "hops_away": 0, "rssi": None, "snr": None,
                "path": "radio", "error": None if ok else "no_receipt",
                "receivers": list(receivers),
            }

            # Check iOS
            if src == "Pager" or "Pager" in receivers:
                ios_msg = self.ios.check_message(tag, timeout=3)
                if ios_msg:
                    result["ios_receive_time"] = time.time()

            self._log_result(result)

            status = f" {C['g']}OK{C['0']} ({', '.join(receivers)})" if ok \
                else f" {C['r']}FAIL{C['0']}"
            print(status, flush=True)

            if ok:
                self._record_success(src)
            else:
                self._record_failure(src)

            time.sleep(INTER_MSG_DELAY)

    # ─── Report Generation ───────────────────────────────────────────

    def generate_report(self, elapsed):
        """Generate markdown report."""
        lines = []
        lines.append(f"# 7-Device MeshReliable Test Report")
        lines.append(f"")
        lines.append(f"**Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"**Duration**: {elapsed/60:.1f} minutes")
        lines.append(f"**Total messages**: {len(self.results)}")
        lines.append(f"")

        # Device inventory
        lines.append(f"## Devices")
        lines.append(f"")
        lines.append(f"| Name | Node ID | Type | USB |")
        lines.append(f"|------|---------|------|-----|")
        for name in ALL_DEVICE_NAMES:
            nid = self._get_dest_id(name)
            hw = USB_DEVICES.get(name, {}).get("hw") or REMOTE_DEVICES.get(name, {}).get("hw", "?")
            usb = "Yes" if name in USB_DEVICES else "No"
            nid_str = f"0x{nid:08x}" if nid else "N/A"
            lines.append(f"| {name} | {nid_str} | {hw} | {usb} |")
        lines.append(f"")

        # Per-phase success rates
        lines.append(f"## Per-Phase Results")
        lines.append(f"")
        lines.append(f"| Phase | Sent | OK | Fail | Rate |")
        lines.append(f"|-------|------|----|------|------|")
        total_sent = 0
        total_ok = 0
        for phase in ["text_dm", "voice_dm", "image_dm", "group_text",
                       "group_voice", "group_image", "channel_broadcast"]:
            s = self.phase_stats[phase]
            rate = (s["ok"] / s["sent"] * 100) if s["sent"] > 0 else 0
            lines.append(f"| {phase} | {s['sent']} | {s['ok']} | {s['fail']} | {rate:.1f}% |")
            total_sent += s["sent"]
            total_ok += s["ok"]
        overall = (total_ok / total_sent * 100) if total_sent > 0 else 0
        lines.append(f"| **TOTAL** | **{total_sent}** | **{total_ok}** | "
                     f"**{total_sent - total_ok}** | **{overall:.1f}%** |")
        lines.append(f"")

        # Per-device success rates (as sender)
        lines.append(f"## Per-Device (Sender)")
        lines.append(f"")
        lines.append(f"| Device | Sent | OK | Rate |")
        lines.append(f"|--------|------|----|------|")
        device_sender = collections.defaultdict(lambda: {"sent": 0, "ok": 0})
        for r in self.results:
            device_sender[r["src"]]["sent"] += 1
            if r["success"]:
                device_sender[r["src"]]["ok"] += 1
        for name in sorted(device_sender.keys()):
            ds = device_sender[name]
            rate = (ds["ok"] / ds["sent"] * 100) if ds["sent"] > 0 else 0
            lines.append(f"| {name} | {ds['sent']} | {ds['ok']} | {rate:.1f}% |")
        lines.append(f"")

        # Per-device success rates (as receiver)
        lines.append(f"## Per-Device (Receiver)")
        lines.append(f"")
        lines.append(f"| Device | Targeted | Received | Rate |")
        lines.append(f"|--------|----------|----------|------|")
        device_recv = collections.defaultdict(lambda: {"sent": 0, "ok": 0})
        for r in self.results:
            dst = r.get("dst", "ALL")
            if dst != "ALL":
                device_recv[dst]["sent"] += 1
                if r["success"]:
                    device_recv[dst]["ok"] += 1
        for name in sorted(device_recv.keys()):
            dr = device_recv[name]
            rate = (dr["ok"] / dr["sent"] * 100) if dr["sent"] > 0 else 0
            lines.append(f"| {name} | {dr['sent']} | {dr['ok']} | {rate:.1f}% |")
        lines.append(f"")

        # Per-pair success rates
        lines.append(f"## Per-Pair Results")
        lines.append(f"")
        lines.append(f"| Pair | Sent | OK | Rate |")
        lines.append(f"|------|------|----|------|")
        pair_stats = collections.defaultdict(lambda: {"sent": 0, "ok": 0})
        for r in self.results:
            pk = f"{r['src']}->{r.get('dst', 'ALL')}"
            pair_stats[pk]["sent"] += 1
            if r["success"]:
                pair_stats[pk]["ok"] += 1
        for pk in sorted(pair_stats.keys()):
            ps = pair_stats[pk]
            rate = (ps["ok"] / ps["sent"] * 100) if ps["sent"] > 0 else 0
            lines.append(f"| {pk} | {ps['sent']} | {ps['ok']} | {rate:.1f}% |")
        lines.append(f"")

        # Latency stats
        lines.append(f"## Latency Stats")
        lines.append(f"")
        lines.append(f"| Phase | Avg (s) | Median (s) | Max (s) | Samples |")
        lines.append(f"|-------|---------|------------|---------|---------|")
        for phase in ["text_dm", "voice_dm", "image_dm", "group_text",
                       "group_voice", "group_image", "channel_broadcast"]:
            lats = [r["latency_s"] for r in self.results
                    if r["phase"] == phase and r["latency_s"] is not None]
            if lats:
                avg = sum(lats) / len(lats)
                lats_sorted = sorted(lats)
                med = lats_sorted[len(lats_sorted) // 2]
                mx = max(lats)
                lines.append(f"| {phase} | {avg:.1f} | {med:.1f} | {mx:.1f} | {len(lats)} |")
            else:
                lines.append(f"| {phase} | — | — | — | 0 |")
        lines.append(f"")

        # Path distribution
        lines.append(f"## Path Distribution")
        lines.append(f"")
        radio = sum(1 for r in self.results if r.get("path") == "radio" and r["success"])
        mqtt_path = sum(1 for r in self.results if r.get("path") == "mqtt" and r["success"])
        lines.append(f"- Radio: {radio}")
        lines.append(f"- MQTT: {mqtt_path}")
        lines.append(f"")

        # iOS delivery stats
        ios_targeted = sum(1 for r in self.results
                          if r["src"] == "Pager" or r.get("dst") == "Pager")
        ios_delivered = sum(1 for r in self.results if r.get("ios_receive_time"))
        lines.append(f"## iOS Delivery")
        lines.append(f"")
        lines.append(f"- Messages involving Pager: {ios_targeted}")
        lines.append(f"- Confirmed on iOS: {ios_delivered}")
        if ios_targeted > 0:
            lines.append(f"- iOS delivery rate: {ios_delivered/ios_targeted*100:.1f}%")
        lines.append(f"")

        # Failed message details
        failures = [r for r in self.results if not r["success"]]
        if failures:
            lines.append(f"## Failed Messages ({len(failures)})")
            lines.append(f"")
            lines.append(f"| # | Phase | Src | Dst | Error |")
            lines.append(f"|---|-------|-----|-----|-------|")
            for i, r in enumerate(failures[:50]):  # cap at 50
                lines.append(f"| {i+1} | {r['phase']} | {r['src']} | {r.get('dst','ALL')} | "
                           f"{r.get('error', 'unknown')} |")
            if len(failures) > 50:
                lines.append(f"| ... | ... | ... | ... | ({len(failures) - 50} more) |")

        report = "\n".join(lines)

        try:
            with open(REPORT_FILE, "w") as f:
                f.write(report)
            log(f"Report saved: {REPORT_FILE}")
        except Exception as e:
            log(f"Report save failed: {e}", "ERROR")

        return report

    # ─── Print Summary ───────────────────────────────────────────────

    def print_summary(self):
        print(f"\n{'='*70}")
        print(f"  SUMMARY")
        print(f"{'='*70}")
        for phase in ["text_dm", "voice_dm", "image_dm", "group_text",
                       "group_voice", "group_image", "channel_broadcast"]:
            s = self.phase_stats[phase]
            if s["sent"] == 0:
                continue
            rate = s["ok"] / s["sent"] * 100
            rc = C["g"] if rate >= 95 else C["y"] if rate >= 80 else C["r"]
            print(f"  {phase:<22s}: {rc}{s['ok']}/{s['sent']} ({rate:.1f}%){C['0']}")
        total_s = sum(s["sent"] for s in self.phase_stats.values())
        total_o = sum(s["ok"] for s in self.phase_stats.values())
        if total_s > 0:
            rate = total_o / total_s * 100
            rc = C["g"] if rate >= 95 else C["y"] if rate >= 80 else C["r"]
            print(f"  {'TOTAL':<22s}: {rc}{total_o}/{total_s} ({rate:.1f}%){C['0']}")
        print(f"{'='*70}\n")

    # ─── Main Run ────────────────────────────────────────────────────

    def run(self):
        if not self.setup():
            return

        start_time = time.time()
        group_only = os.environ.get("GROUP_ONLY") == "1"
        media_only = os.environ.get("MEDIA_ONLY") == "1"

        try:
            if media_only:
                # Media-focused run: voice + image DMs only (with retransmit + spacing).
                log(f"\n{'='*70}\n  MEDIA-ONLY RUN — voice + image DMs"
                    f"  (max {MEDIA_MAX_ATTEMPTS} attempts/transfer, {MEDIA_SEND_DELAY}s spacing)\n{'='*70}")
                self.run_phase_voice_dm()
                self._reconcile_phase("voice_dm")
                self.print_summary()
                self._reset_phase()
                self.run_phase_image_dm()
                self._reconcile_phase("image_dm")
                self.print_summary()
                self._reset_phase()
                return  # `finally` block generates the report

            if group_only:
                # Group-text-focused run: setup + group text only.
                members = self._get_all_ids()
                online = self._online_member_ids(members)
                log(f"\n{'='*70}\n  GROUP-ONLY RUN — group text")
                log(f"  Members: {len(members)}  |  online (<10min): {len(online)} "
                    f"-> {[f'0x{m:08x}' for m in online]}\n{'='*70}")
                for _ in range(int(os.environ.get("GROUP_ROUNDS", "1"))):
                    self.run_phase_group_text()
                    self._reconcile_phase("group_text")
                    self.print_summary()
                    self._reset_phase()
                return  # `finally` block generates the report

            # Phase 2: Text DMs
            self.run_phase_text_dm()
            self._reconcile_phase("text_dm")
            self.print_summary()
            self._reset_phase()

            # Phase 3: Voice DMs
            self.run_phase_voice_dm()
            self._reconcile_phase("voice_dm")
            self.print_summary()
            self._reset_phase()

            # Phase 4: Image DMs
            self.run_phase_image_dm()
            self._reconcile_phase("image_dm")
            self.print_summary()
            self._reset_phase()

            # Phase 5: Group setup (implicit — members from _get_all_ids)
            log(f"\n{'='*70}")
            log(f"  PHASE 5: GROUP SETUP")
            log(f"{'='*70}")
            members = self._get_all_ids()
            log(f"  Group members: {len(members)} nodes")
            for m in members:
                log(f"    0x{m:08x}")
            log(f"  Group ID (maluca hash): 0x{self.maluca_hash:08x}" if self.maluca_hash else "  No group hash")

            # Phase 6: Group text
            self.run_phase_group_text()
            self._reconcile_phase("group_text")
            self.print_summary()
            self._reset_phase()

            # Phase 7: Group voice
            self.run_phase_group_voice()
            self._reconcile_phase("group_voice")
            self.print_summary()
            self._reset_phase()

            # Phase 8: Group image
            self.run_phase_group_image()
            self._reconcile_phase("group_image")
            self.print_summary()
            self._reset_phase()

            # Phase 9: Channel broadcasts
            self.run_phase_channel_broadcast()
            self._reconcile_phase("channel_broadcast")
            self.print_summary()

        except KeyboardInterrupt:
            log("\nInterrupted by user", "WARN")
        except Exception as e:
            log(f"Unexpected error: {e}", "ERROR")
            traceback.print_exc()
        finally:
            elapsed = time.time() - start_time
            log(f"\nTest completed in {elapsed/60:.1f} minutes")

            # Final summary
            self.print_summary()

            # Generate report
            self.generate_report(elapsed)

            log(f"JSONL: {JSONL_FILE}")
            log(f"Report: {REPORT_FILE}")

            # Cleanup
            try:
                pub.unsubscribe(self._on_rx, "meshtastic.receive")
            except Exception:
                pass
            try:
                pub.unsubscribe(self._on_disconnect, "meshtastic.connection.lost")
            except Exception:
                pass
            for iface in self.interfaces.values():
                try:
                    iface.close()
                except Exception:
                    pass


def main():
    Full7DeviceTest().run()


if __name__ == "__main__":
    main()
