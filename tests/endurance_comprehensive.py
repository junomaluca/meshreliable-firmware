#!/usr/bin/env python3
"""Comprehensive endurance test: ALL message types across ALL device pairs.

Tests text DMs, channel broadcasts, image transfers, voice memo transfers,
and group-style multi-DMs. Each with proper receive verification via serial
callbacks (not just send-success).

Runs until rolling 100-test rate >= target across ALL categories, or forever.

Usage:
  python3 endurance_comprehensive.py --target-rate 99
  python3 endurance_comprehensive.py --count 50  # fixed count baseline
"""
import sys, os, time, random, threading, argparse, json, collections, struct
import meshtastic
import meshtastic.serial_interface
from pubsub import pub

# ─── Device Configuration ────────────────────────────────────────────────
DEVICES = {
    'VHF-A': {'port': '/dev/cu.usbmodem101',  'id': None},
    'VHF-B': {'port': '/dev/cu.usbmodem1101', 'id': None},
    'BPF-A': {'port': '/dev/cu.usbmodem21101', 'id': None},
    'BPF-B': {'port': '/dev/cu.usbmodem21201', 'id': None},
}

PAIRS = [
    ('VHF-A', 'VHF-B'), ('VHF-B', 'VHF-A'),
    ('VHF-A', 'BPF-A'), ('BPF-A', 'VHF-A'),
    ('VHF-A', 'BPF-B'), ('BPF-B', 'VHF-A'),
    ('VHF-B', 'BPF-A'), ('BPF-A', 'VHF-B'),
    ('VHF-B', 'BPF-B'), ('BPF-B', 'VHF-B'),
    ('BPF-A', 'BPF-B'), ('BPF-B', 'BPF-A'),
]

# ANSI colors
C = {'g': '\033[92m', 'y': '\033[93m', 'r': '\033[91m', 'b': '\033[1m',
     'd': '\033[2m', '0': '\033[0m'}

# ─── Protobuf Helpers ────────────────────────────────────────────────────
def _ev(v):
    buf = bytearray()
    if v == 0: buf.append(0); return buf
    while v > 0x7F: buf.append((v & 0x7F) | 0x80); v >>= 7
    buf.append(v & 0x7F)
    return buf
def _fv(fn, val):
    if val == 0: return bytearray()
    return _ev((fn << 3) | 0) + _ev(val)
def _fb(fn, val):
    if not val: return bytearray()
    return _ev((fn << 3) | 2) + _ev(len(val)) + bytearray(val)
def _crc32(data):
    crc = 0xFFFFFFFF
    for b in data:
        crc ^= b
        for _ in range(8): crc = (crc >> 1) ^ 0xEDB88320 if crc & 1 else crc >> 1
    return (~crc) & 0xFFFFFFFF
def _dec_fields(payload):
    fields = {}
    if not isinstance(payload, (bytes, bytearray)) or not payload: return fields
    pos = 0
    while pos < len(payload):
        tw = 0; s = 0
        while pos < len(payload):
            b = payload[pos]; pos += 1; tw |= (b & 0x7F) << s; s += 7
            if not (b & 0x80): break
        fn = tw >> 3; wt = tw & 7
        if wt == 0:
            v = 0; s = 0
            while pos < len(payload):
                b = payload[pos]; pos += 1; v |= (b & 0x7F) << s; s += 7
                if not (b & 0x80): break
            fields[fn] = v
        elif wt == 2:
            ln = 0; s = 0
            while pos < len(payload):
                b = payload[pos]; pos += 1; ln |= (b & 0x7F) << s; s += 7
                if not (b & 0x80): break
            fields[fn] = payload[pos:pos+ln]; pos += ln
        elif wt == 5: fields[fn] = payload[pos:pos+4]; pos += 4
        elif wt == 1: fields[fn] = payload[pos:pos+8]; pos += 8
        else: break
    return fields


# ─── Test Categories ─────────────────────────────────────────────────────
CATEGORIES = ['text_dm', 'channel_broadcast', 'image_transfer', 'voice_transfer', 'group_dm']


def _rc(rate):
    if rate >= 95: return C['g']
    if rate >= 80: return C['y']
    return C['r']


class ComprehensiveEndurance:
    def __init__(self, args):
        self.count = args.count  # 0 = infinite
        self.delay = args.delay
        self.target_rate = args.target_rate
        self.skip = set(s.strip() for s in (args.skip_senders or '').split(',') if s.strip())
        self.interfaces = {}
        self.iface_to_name = {}  # interface object -> device name
        self.lock = threading.Lock()
        self.running = True
        self.start_time = None
        self.logfile = f"/tmp/endurance_{int(time.time())}.jsonl"
        self.learnings_file = f"/tmp/endurance_learnings_{int(time.time())}.md"

        # Per-category rolling stats (last 100 per category)
        self.rolling = {cat: collections.deque(maxlen=100) for cat in CATEGORIES}
        self.cumulative = {cat: {'sent': 0, 'ok': 0} for cat in CATEGORIES}

        # Per-pair stats
        self.pair_stats = {}
        for s, d in PAIRS:
            k = f"{s}->{d}"
            self.pair_stats[k] = {cat: {'sent': 0, 'ok': 0} for cat in CATEGORIES}

        # Pending receive verifications
        self.pending_rx = {}  # tracking_id -> {'event': Event, 'received_by': set(), ...}

        # Pending media ACKs
        self.pending_ack = {}

        # Signal quality
        self.signal = {}  # pair -> deque of {rssi, snr}
        for s, d in PAIRS:
            self.signal[f"{s}->{d}"] = collections.deque(maxlen=20)

        # Learnings accumulated during run
        self.learnings = []

    # ─── Setup ────────────────────────────────────────────────────────

    def setup(self):
        print(f"{'='*70}")
        print(f"  COMPREHENSIVE ENDURANCE TEST")
        cnt = "INFINITE" if self.count == 0 else str(self.count)
        print(f"  Count: {cnt}, Delay: {self.delay}s, Target: {self.target_rate}%")
        if self.skip: print(f"  Skip: {', '.join(self.skip)}")
        print(f"  Log: {self.logfile}")
        print(f"  Learnings: {self.learnings_file}")
        print(f"{'='*70}\n")

        pub.subscribe(self._on_rx, "meshtastic.receive")
        pub.subscribe(self._on_disconnect, "meshtastic.connection.lost")

        for name in ['VHF-A', 'VHF-B', 'BPF-A', 'BPF-B']:
            if name in self.skip:
                print(f"  Skipping {name}", flush=True)
                continue
            port = DEVICES[name]['port']
            print(f"  Opening {name} ({port})...", flush=True)
            try:
                iface = meshtastic.serial_interface.SerialInterface(port)
                time.sleep(3)
                node = iface.getMyNodeInfo()
                num = node.get('num', 0)
                DEVICES[name]['id'] = num
                self.interfaces[name] = iface
                self.iface_to_name[id(iface)] = name
                hw = node.get('user', {}).get('hwModel', '?')
                up = node.get('deviceMetrics', {}).get('uptimeSeconds', '?')
                ch_util = node.get('deviceMetrics', {}).get('channelUtilization', '?')
                print(f"    -> 0x{num:08x} hw={hw} up={up}s ch_util={ch_util}%", flush=True)
            except Exception as e:
                print(f"    -> FAILED: {e}", flush=True)

        active = [n for n in DEVICES if n in self.interfaces]
        print(f"\n  Active: {', '.join(active)} ({len(active)}/4)")
        if len(active) < 2:
            print("  *** Need >= 2 devices ***")
            return False

        print(f"  Settling 10s...", flush=True)
        time.sleep(10)

        # Warmup: send small media transfers to prime radio path
        active_names = [n for n in DEVICES if n in self.interfaces]
        if len(active_names) >= 2:
            s, d = active_names[0], active_names[1]
            print(f"  Warmup: {s}->{d}...", end=' ', flush=True)
            tid, ok, _ = self._send_media(s, d, media_type=3, size=10)
            print("OK" if ok else "skip", flush=True)
            time.sleep(3)
            print(f"  Warmup: {d}->{s}...", end=' ', flush=True)
            tid, ok, _ = self._send_media(d, s, media_type=3, size=10)
            print("OK" if ok else "skip", flush=True)
            time.sleep(3)

        print(flush=True)
        return True

    # ─── Receive Callback ─────────────────────────────────────────────

    def _on_rx(self, packet, interface):
        """Global receive handler for ALL interfaces."""
        iface_id = id(interface)
        receiver = self.iface_to_name.get(iface_id, '?')
        decoded = packet.get("decoded", {})
        portnum = decoded.get("portnum", "?")
        payload = decoded.get("payload", b"")
        rssi = packet.get('rxRssi') or packet.get('rssi')
        snr = packet.get('rxSnr') or packet.get('snr')
        from_id = packet.get('fromId', '') or ''

        # Text message received — check pending verifications
        if portnum in ("TEXT_MESSAGE_APP", 1):
            text = ""
            if isinstance(payload, bytes):
                try: text = payload.decode('utf-8')
                except: text = ""
            elif isinstance(payload, str):
                text = payload

            with self.lock:
                for tid, info in list(self.pending_rx.items()):
                    if tid in text and receiver != info.get('sender'):
                        info['received_by'].add(receiver)
                        if rssi is not None:
                            info['rssi'] = rssi
                            info['snr'] = snr
                        if not info['event'].is_set():
                            # For DMs: need specific receiver; for broadcast: any
                            if info.get('expect_receiver'):
                                if receiver == info['expect_receiver']:
                                    info['event'].set()
                            else:
                                info['event'].set()

        # Media ACK/NACK
        if portnum in (259, "MEDIA_TRANSFER_APP"):
            mtype_str = self._decode_media_type(payload)
            tid_val = self._decode_tid(payload)

            # Log media packets for debugging failed transfers
            ts_dbg = time.strftime("%H:%M:%S")
            if mtype_str in ("START", "ACK_COMPLETE", "NACK"):
                print(f"  {C['d']}[MX {ts_dbg}] {receiver}<-{from_id} {mtype_str} "
                      f"tid={tid_val} rssi={rssi}{C['0']}", flush=True)

            if mtype_str in ("ACK_COMPLETE", "NACK"):
                with self.lock:
                    matched = False
                    if tid_val and tid_val in self.pending_ack:
                        state = self.pending_ack[tid_val]
                        if not state['event'].is_set():
                            state['result'] = mtype_str
                            state['rx_time'] = time.time()
                            state['rssi'] = rssi
                            state['snr'] = snr
                            state['event'].set()
                            matched = True

                    if not matched:
                        for tid, state in self.pending_ack.items():
                            if not state['event'].is_set():
                                state['result'] = mtype_str
                                state['rx_time'] = time.time()
                                state['rssi'] = rssi
                                state['snr'] = snr
                                state['event'].set()
                                break

    @staticmethod
    def _decode_media_type(payload):
        f = _dec_fields(payload)
        t = f.get(1, None)
        if t is not None:
            return {0:"CHUNK",1:"START",2:"COMPLETE",3:"NACK",4:"ACK_COMPLETE",5:"CANCEL"}.get(t, f"?{t}")
        return "CHUNK"

    @staticmethod
    def _decode_tid(payload):
        return _dec_fields(payload).get(2, None)

    # ─── Test Methods ─────────────────────────────────────────────────

    def test_text_dm(self, src, dst):
        """Send text DM, verify receipt on destination via serial callback (with retry)."""
        ok, lat = self._send_text_dm_once(src, dst)
        if not ok:
            time.sleep(3)
            ok, lat = self._send_text_dm_once(src, dst)
            if ok:
                self._record_learning(f"TEXT_DM retry succeeded: {src}->{dst}")
        return ok, lat

    def _send_text_dm_once(self, src, dst, timeout=20):
        """Single attempt to send text DM and verify receipt.
        Uses wantAck=True for firmware-level retry (3 auto-retries at ~8s intervals).
        The 20s timeout allows firmware 2 retry cycles before Python gives up."""
        iface = self.interfaces.get(src)
        dest_id = DEVICES[dst]['id']
        if not iface or not dest_id:
            return False, None

        tid = f"DM-{src[0]}{dst[0]}-{int(time.time())%100000}-{random.randint(100,999)}"
        msg = f"{tid} txt"

        event = threading.Event()
        with self.lock:
            self.pending_rx[tid] = {
                'event': event, 'sender': src, 'expect_receiver': dst,
                'received_by': set(), 'rssi': None, 'snr': None,
            }

        send_time = time.time()
        try:
            iface.sendText(msg, destinationId=dest_id, wantAck=True, wantResponse=False)
        except Exception as e:
            with self.lock: self.pending_rx.pop(tid, None)
            self._reconnect(src)
            return False, None

        got = event.wait(timeout=timeout)
        with self.lock:
            info = self.pending_rx.pop(tid, {})

        if got and dst in info.get('received_by', set()):
            lat = time.time() - send_time
            self._record_signal(src, dst, info)
            return True, lat
        return False, None

    def test_channel_broadcast(self, src):
        """Send channel broadcast, verify at least one other device receives it."""
        iface = self.interfaces.get(src)
        if not iface:
            return False, None

        tid = f"CH-{src[0]}-{int(time.time())%100000}-{random.randint(100,999)}"
        msg = f"{tid} bcast"

        event = threading.Event()
        with self.lock:
            self.pending_rx[tid] = {
                'event': event, 'sender': src, 'expect_receiver': None,
                'received_by': set(), 'rssi': None, 'snr': None,
            }

        send_time = time.time()
        try:
            iface.sendText(msg, wantAck=False, wantResponse=False)
        except Exception as e:
            with self.lock: self.pending_rx.pop(tid, None)
            self._reconnect(src)
            return False, None

        got = event.wait(timeout=15)
        with self.lock:
            info = self.pending_rx.pop(tid, {})

        receivers = info.get('received_by', set()) - {src}
        if receivers:
            lat = time.time() - send_time
            return True, lat
        return False, None

    def test_image_transfer(self, src, dst):
        """Send image-typed media transfer, verify ACK_COMPLETE (with retry + double CANCEL)."""
        tid, ok, lat = self._send_media(src, dst, media_type=1, size=random.choice([20, 50, 100]))
        if not ok:
            self._send_cancel(src, dst, tid)
            time.sleep(5)  # extra wait for receiver transfer timeout
            _, ok, lat = self._send_media(src, dst, media_type=1, size=random.choice([20, 50]))
            if ok:
                self._record_learning(f"IMAGE retry succeeded: {src}->{dst}")
        return ok, lat

    def test_voice_transfer(self, src, dst):
        """Send voice-memo-typed media transfer, verify ACK_COMPLETE (with retry + double CANCEL)."""
        tid, ok, lat = self._send_media(src, dst, media_type=2, size=random.choice([20, 50, 80]))
        if not ok:
            self._send_cancel(src, dst, tid)
            time.sleep(5)  # extra wait for receiver transfer timeout
            _, ok, lat = self._send_media(src, dst, media_type=2, size=random.choice([20, 50]))
            if ok:
                self._record_learning(f"VOICE retry succeeded: {src}->{dst}")
        return ok, lat

    def _send_cancel(self, src, dst, tid):
        """Send CANCEL packet twice to clear stale transfer state on receiver."""
        if not tid:
            return
        iface = self.interfaces.get(src)
        dest_id = DEVICES[dst]['id']
        if not iface or not dest_id:
            return
        cancel_pkt = bytearray()
        cancel_pkt.extend(_fv(1, 5))  # type=CANCEL
        cancel_pkt.extend(_fv(2, tid))
        try:
            iface.sendData(bytes(cancel_pkt), destinationId=dest_id, portNum=259,
                           wantAck=False, wantResponse=False)
            time.sleep(3)
            # Send CANCEL again in case first was lost
            iface.sendData(bytes(cancel_pkt), destinationId=dest_id, portNum=259,
                           wantAck=False, wantResponse=False)
        except:
            pass
        time.sleep(3)  # let second CANCEL propagate

    def test_group_dm(self, src, destinations):
        """Send text DM to multiple destinations (group-style), verify all receive (with retry)."""
        iface = self.interfaces.get(src)
        if not iface:
            return False, None

        successes = 0
        send_time = time.time()

        for idx, dst in enumerate(destinations):
            dest_id = DEVICES[dst]['id']
            if not dest_id:
                continue

            # First attempt
            ok = self._send_group_member(src, dst, idx, iface, dest_id)
            if not ok:
                time.sleep(3)
                ok = self._send_group_member(src, dst, idx, iface, dest_id)
                if ok:
                    self._record_learning(f"GROUP_DM retry succeeded: {src}->{dst}")
            if ok:
                successes += 1
            time.sleep(3)  # spacing between group members

        total_lat = time.time() - send_time
        return successes == len(destinations), total_lat

    def _send_group_member(self, src, dst, idx, iface, dest_id):
        """Send one group DM to a single member, verify receipt."""
        sub_tid = f"GRP-{src[0]}-{int(time.time())%100000}-{random.randint(100,999)}-{idx}"
        msg = f"{sub_tid} grp"
        event = threading.Event()
        with self.lock:
            self.pending_rx[sub_tid] = {
                'event': event, 'sender': src, 'expect_receiver': dst,
                'received_by': set(), 'rssi': None, 'snr': None,
            }
        try:
            iface.sendText(msg, destinationId=dest_id, wantAck=False, wantResponse=False)
        except Exception as e:
            with self.lock: self.pending_rx.pop(sub_tid, None)
            return False

        got = event.wait(timeout=12)
        with self.lock:
            info = self.pending_rx.pop(sub_tid, {})
        if got and dst in info.get('received_by', set()):
            self._record_signal(src, dst, info)
            return True
        return False

    def _send_media(self, src, dst, media_type, size):
        """Send media transfer with specified type. Returns (tid, ok, latency)."""
        iface = self.interfaces.get(src)
        dest_id = DEVICES[dst]['id']
        if not iface or not dest_id:
            return None, False, None

        data = bytes([(i * 37 + random.randint(0, 255)) & 0xFF for i in range(size)])
        checksum = _crc32(data)
        tid = random.randint(0x10000, 0xFFFFFFF)

        start_pkt = bytearray()
        start_pkt.extend(_fv(1, 1))  # type=START
        start_pkt.extend(_fv(2, tid))
        start_pkt.extend(_fv(4, 1))  # totalChunks
        start_pkt.extend(_fv(5, size))
        start_pkt.extend(_fv(7, media_type))  # contentType
        start_pkt.extend(_fv(9, checksum))

        chunk_pkt = bytearray()
        chunk_pkt.extend(_fv(2, tid))
        chunk_pkt.extend(_fb(6, data))

        complete_pkt = bytearray()
        complete_pkt.extend(_fv(1, 2))  # type=COMPLETE
        complete_pkt.extend(_fv(2, tid))
        complete_pkt.extend(_fv(9, checksum))

        ack_event = threading.Event()
        with self.lock:
            self.pending_ack[tid] = {
                'event': ack_event, 'result': None,
                'time': time.time(), 'rx_time': None,
                'rssi': None, 'snr': None,
            }

        try:
            iface.sendData(bytes(start_pkt), destinationId=dest_id, portNum=259,
                           wantAck=False, wantResponse=False)
            time.sleep(8)
            iface.sendData(bytes(chunk_pkt), destinationId=dest_id, portNum=259,
                           wantAck=False, wantResponse=False)
            time.sleep(8)
            iface.sendData(bytes(complete_pkt), destinationId=dest_id, portNum=259,
                           wantAck=False, wantResponse=False)
        except Exception as e:
            with self.lock: self.pending_ack.pop(tid, None)
            self._reconnect(src)
            return tid, False, None

        got = ack_event.wait(timeout=45)
        with self.lock:
            state = self.pending_ack.pop(tid, None)

        if state and state['result'] == 'ACK_COMPLETE':
            lat = state['rx_time'] - state['time']
            self._record_signal(src, dst, state)
            return tid, True, lat
        return tid, False, None

    # ─── Helpers ──────────────────────────────────────────────────────

    def _record_signal(self, src, dst, info):
        pk = f"{src}->{dst}"
        rssi = info.get('rssi')
        snr = info.get('snr')
        if rssi is not None:
            self.signal[pk].append({'rssi': rssi, 'snr': snr, 't': time.time()})

    def _on_disconnect(self, interface):
        """Auto-detect serial disconnects and remove stale interfaces."""
        iface_id = id(interface)
        name = self.iface_to_name.get(iface_id)
        if name:
            ts = time.strftime("%H:%M:%S")
            print(f"  [{ts}] {name} serial lost (auto-detected)", flush=True)
            self.iface_to_name.pop(iface_id, None)
            self.interfaces.pop(name, None)
            # Force-close serial port to release the file descriptor
            try:
                if hasattr(interface, 'stream') and interface.stream:
                    interface.stream.close()
            except: pass
            try:
                interface.close()
            except: pass

    def _ensure_connected(self, name):
        """Verify device is connected, reconnect if needed. Returns True if connected."""
        if name not in self.interfaces:
            return self._reconnect(name)
        iface = self.interfaces[name]
        try:
            # Check if meshtastic library flagged as disconnected
            if hasattr(iface, 'isConnected') and not iface.isConnected.is_set():
                return self._reconnect(name)
            # Check serial stream is still open
            if hasattr(iface, 'stream') and hasattr(iface.stream, 'is_open') and not iface.stream.is_open:
                return self._reconnect(name)
            return True
        except:
            return self._reconnect(name)

    def _reconnect(self, name):
        port = DEVICES[name]['port']
        ts = time.strftime("%H:%M:%S")
        print(f"  [{ts}] Reconnecting {name}...", flush=True)
        old = self.interfaces.pop(name, None)
        if old:
            old_id = id(old)
            self.iface_to_name.pop(old_id, None)
            # Force-close serial port first, then interface
            try:
                if hasattr(old, 'stream') and old.stream:
                    old.stream.close()
            except: pass
            try: old.close()
            except: pass
        time.sleep(8)  # Wait for OS to release serial port
        for attempt in range(5):
            try:
                iface = meshtastic.serial_interface.SerialInterface(port)
                time.sleep(4)
                node = iface.getMyNodeInfo()
                num = node.get('num', 0)
                DEVICES[name]['id'] = num
                self.interfaces[name] = iface
                self.iface_to_name[id(iface)] = name
                print(f"  [{ts}] {name} reconnected", flush=True)
                return True
            except Exception as e:
                print(f"  [{ts}] {name} attempt {attempt+1}/5 failed: {e}", flush=True)
                time.sleep(10)
        return False

    def _record_learning(self, text):
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        entry = f"- [{ts}] {text}"
        self.learnings.append(entry)
        with open(self.learnings_file, 'a') as f:
            f.write(entry + '\n')

    # ─── Stats ────────────────────────────────────────────────────────

    def _cat_rolling_rate(self, cat):
        d = self.rolling[cat]
        if not d: return 0.0
        return sum(1 for x in d if x) / len(d) * 100

    def _cat_cum_rate(self, cat):
        c = self.cumulative[cat]
        return (c['ok'] / c['sent'] * 100) if c['sent'] > 0 else 0.0

    def _all_at_target(self):
        """Check if ALL categories with >= 20 samples are at target."""
        if not self.target_rate:
            return False
        for cat in CATEGORIES:
            if len(self.rolling[cat]) >= 20:
                if self._cat_rolling_rate(cat) < self.target_rate:
                    return False
        # Need at least 20 samples in each
        if any(len(self.rolling[cat]) < 20 for cat in CATEGORIES):
            return False
        return True

    def print_stats(self, test_num=None):
        ts = time.strftime("%H:%M:%S")
        elapsed = time.time() - self.start_time
        h = int(elapsed // 3600)
        m = int((elapsed % 3600) // 60)

        print(f"\n{'='*70}")
        print(f"  COMPREHENSIVE ENDURANCE @ {ts} (elapsed: {h}h{m:02d}m)")
        if test_num:
            cnt = f"/{self.count}" if self.count > 0 else ""
            print(f"  Test #{test_num}{cnt}")
        print(f"{'='*70}")

        # Per-category summary
        print(f"\n  {'Category':<22s} {'Rolling(100)':<15s} {'Cumulative':<15s} {'Sent':>5s}")
        print(f"  {'-'*60}")
        all_ok = True
        for cat in CATEGORIES:
            rr = self._cat_rolling_rate(cat)
            cr = self._cat_cum_rate(cat)
            sent = self.cumulative[cat]['sent']
            rc = _rc(rr); cc = _rc(cr); r = C['0']
            check = '✓' if rr >= (self.target_rate or 99) and len(self.rolling[cat]) >= 20 else ' '
            print(f"  {cat:<22s} {rc}{rr:>6.1f}%{r} ({len(self.rolling[cat]):>3d})  "
                  f"{cc}{cr:>6.1f}%{r}        {sent:>5d}  {check}")
            if rr < (self.target_rate or 99):
                all_ok = False

        if all_ok and all(len(self.rolling[c]) >= 20 for c in CATEGORIES):
            print(f"\n  {C['b']}*** ALL CATEGORIES AT TARGET! ***{C['0']}")

        # Per-pair breakdown (aggregated across categories)
        print(f"\n  Per-pair (all categories):")
        for pk in sorted(self.pair_stats.keys()):
            total_sent = sum(self.pair_stats[pk][c]['sent'] for c in CATEGORIES)
            total_ok = sum(self.pair_stats[pk][c]['ok'] for c in CATEGORIES)
            if total_sent == 0:
                continue
            rate = total_ok / total_sent * 100
            rc = _rc(rate)
            # Signal
            sig = ""
            if self.signal.get(pk):
                recent = list(self.signal[pk])[-5:]
                rssis = [s['rssi'] for s in recent if s.get('rssi') is not None]
                snrs = [s['snr'] for s in recent if s.get('snr') is not None]
                if rssis:
                    sig = f" rssi={sum(rssis)/len(rssis):.0f}"
                if snrs:
                    sig += f" snr={sum(snrs)/len(snrs):.1f}"
            print(f"    {pk:15s}: {rc}{total_ok}/{total_sent} ({rate:.0f}%){C['0']}{sig}")

        print(f"{'='*70}\n")

    # ─── Main Loop ────────────────────────────────────────────────────

    def run(self):
        if not self.setup():
            return

        self.start_time = time.time()
        active = [n for n in DEVICES if n in self.interfaces and n not in self.skip]
        pairs = [(s, d) for s, d in PAIRS
                 if s in self.interfaces and d in self.interfaces
                 and s not in self.skip]

        if not pairs:
            print("  No valid pairs!")
            return

        infinite = self.count == 0
        i = 0

        # Test cycle: rotate through all 5 categories
        # 0=text_dm, 1=image_transfer, 2=voice_transfer, 3=group_dm, 4=channel_broadcast
        # Channel broadcast moved to END to avoid rebroadcast collision with image
        try:
            while True:
                i += 1
                if not infinite and i > self.count:
                    break

                # Check if target reached
                if self._all_at_target():
                    print(f"\n{C['b']}=== ALL CATEGORIES AT {self.target_rate}%! ==={C['0']}")
                    self._record_learning(
                        f"TARGET REACHED: All categories >= {self.target_rate}% after {i-1} tests")
                    self.print_stats(i - 1)
                    break

                cycle = i % 5
                ts = time.strftime("%H:%M:%S")
                pair = pairs[(i // 5) % len(pairs)]
                src, dst = pair
                pk = f"{src}->{dst}"

                # Pre-test connection check for devices involved
                if not self._ensure_connected(src):
                    print(f"[{ts}] #{i}: SKIP ({src} disconnected)", flush=True)
                    time.sleep(self.delay)
                    continue
                if cycle != 4 and not self._ensure_connected(dst):  # channel doesn't need dst
                    print(f"[{ts}] #{i}: SKIP ({dst} disconnected)", flush=True)
                    time.sleep(self.delay)
                    continue

                if cycle == 0:  # text_dm
                    cat = 'text_dm'
                    print(f"[{ts}] #{i}: TEXT_DM {pk}...", end=' ', flush=True)
                    ok, lat = self.test_text_dm(src, dst)

                elif cycle == 1:  # image_transfer (now follows text_dm, not broadcast)
                    cat = 'image_transfer'
                    print(f"[{ts}] #{i}: IMAGE {pk}...", end=' ', flush=True)
                    ok, lat = self.test_image_transfer(src, dst)

                elif cycle == 2:  # voice_transfer
                    cat = 'voice_transfer'
                    print(f"[{ts}] #{i}: VOICE {pk}...", end=' ', flush=True)
                    ok, lat = self.test_voice_transfer(src, dst)

                elif cycle == 3:  # group_dm
                    cat = 'group_dm'
                    # Pick 2 other devices as group members
                    others = [d for d in active if d != src][:2]
                    if len(others) < 2:
                        others = [d for d in active if d != src]
                    pk_group = f"{src}->{'+'.join(others)}"
                    print(f"[{ts}] #{i}: GROUP {pk_group}...", end=' ', flush=True)
                    ok, lat = self.test_group_dm(src, others)
                    pk = f"{src}->{others[0]}" if others else pk  # log under first dest pair

                else:  # channel_broadcast (moved to end of cycle)
                    cat = 'channel_broadcast'
                    sender = active[i % len(active)]
                    pk = f"{sender}->ALL"
                    print(f"[{ts}] #{i}: CHANNEL {sender}->ALL...", end=' ', flush=True)
                    ok, lat = self.test_channel_broadcast(sender)

                # Extra settling after channel broadcast (rebroadcasts take ~3s)
                if cat == 'channel_broadcast':
                    time.sleep(5)
                # Extra settling after text_dm (wantAck=True ACK needs ~2s)
                if cat == 'text_dm' and ok:
                    time.sleep(3)

                # Record results
                status = f"{C['g']}OK{C['0']} {lat:.1f}s" if ok else f"{C['r']}FAIL{C['0']}"
                print(status, flush=True)

                self.rolling[cat].append(ok)
                self.cumulative[cat]['sent'] += 1
                if ok:
                    self.cumulative[cat]['ok'] += 1

                if pk in self.pair_stats:
                    self.pair_stats[pk][cat]['sent'] += 1
                    if ok:
                        self.pair_stats[pk][cat]['ok'] += 1

                # Log
                entry = {
                    'time': ts, 'epoch': time.time(), 'num': i,
                    'category': cat, 'pair': pk, 'ok': ok, 'latency': lat,
                }
                with open(self.logfile, 'a') as f:
                    f.write(json.dumps(entry) + '\n')

                # Detect and log failures for learnings
                if not ok:
                    fail_rate = self._cat_rolling_rate(cat)
                    if len(self.rolling[cat]) >= 10 and fail_rate < 80:
                        self._record_learning(
                            f"DEGRADATION: {cat} rolling rate {fail_rate:.0f}% on {pk}")

                # Stats output
                if i % 15 == 0:
                    self.print_stats(i)
                elif i % 5 == 0:
                    # Compact status
                    rates = " | ".join(
                        f"{cat[:6]}:{_rc(self._cat_rolling_rate(cat))}"
                        f"{self._cat_rolling_rate(cat):.0f}%{C['0']}"
                        for cat in CATEGORIES if self.cumulative[cat]['sent'] > 0
                    )
                    print(f"  #{i} {rates}", flush=True)

                time.sleep(self.delay)

        except KeyboardInterrupt:
            print("\n\nInterrupted.", flush=True)
            self._record_learning("Test interrupted by user")
        finally:
            self.running = False
            self.print_stats(i)
            self._save_final(i)

            try: pub.unsubscribe(self._on_rx, "meshtastic.receive")
            except: pass
            try: pub.unsubscribe(self._on_disconnect, "meshtastic.connection.lost")
            except: pass
            for iface in self.interfaces.values():
                try: iface.close()
                except: pass

    def _save_final(self, total_tests):
        # Summary
        final = {
            'summary': True,
            'total_tests': total_tests,
            'elapsed': time.time() - self.start_time,
            'cumulative': self.cumulative,
            'pair_stats': {
                pk: {cat: stats for cat, stats in cats.items() if stats['sent'] > 0}
                for pk, cats in self.pair_stats.items()
                if any(s['sent'] > 0 for s in cats.values())
            },
        }
        with open(self.logfile, 'a') as f:
            f.write(json.dumps(final) + '\n')

        # Write learnings summary
        with open(self.learnings_file, 'a') as f:
            f.write(f"\n## Final Summary\n")
            for cat in CATEGORIES:
                rr = self._cat_rolling_rate(cat)
                cr = self._cat_cum_rate(cat)
                f.write(f"- {cat}: rolling={rr:.1f}%, cumulative={cr:.1f}%, "
                        f"sent={self.cumulative[cat]['sent']}\n")
            f.write(f"\n### Per-pair\n")
            for pk in sorted(self.pair_stats.keys()):
                total = sum(s['sent'] for s in self.pair_stats[pk].values())
                ok = sum(s['ok'] for s in self.pair_stats[pk].values())
                if total > 0:
                    f.write(f"- {pk}: {ok}/{total} ({ok/total*100:.0f}%)\n")

        print(f"Results: {self.logfile}")
        print(f"Learnings: {self.learnings_file}")


def main():
    parser = argparse.ArgumentParser(description='Comprehensive endurance test')
    parser.add_argument('--count', type=int, default=0,
                        help='Number of tests (0 = infinite, default: 0)')
    parser.add_argument('--delay', type=float, default=3,
                        help='Delay between tests in seconds (default: 3)')
    parser.add_argument('--target-rate', type=float, default=99.0,
                        help='Target rolling success rate (default: 99.0)')
    parser.add_argument('--skip-senders', type=str, default='',
                        help='Comma-separated device names to skip')
    args = parser.parse_args()
    ComprehensiveEndurance(args).run()


if __name__ == "__main__":
    main()
