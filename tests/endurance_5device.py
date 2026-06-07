#!/usr/bin/env python3
"""5-Device Dual-Band Endurance Test — MeshReliable

Auto-discovers USB devices by hwModel and optionally adds an iPhone
participant via TCP connection to a WiFi-enabled device.

Tests all message types across discovered devices on two frequency bands:
  - 915 MHz: Pager, T3S3, XIAO
  - 144 MHz: T-Beam Supreme, T-Beam BPF
  - Cross-band: pairs via MQTT bridge
  - iPhone: TCP→device→LoRa path (same as real iPhone)

Phased execution:
  Phase A: Smoke test (~50 messages) — all types, gate on success
  Phase B: Same-band endurance (500+ per band)
  Phase C: Cross-band MQTT endurance (200+ messages)
  Phase D: Full mixed endurance (1000+ messages total)

Usage:
  python3 tests/endurance_5device.py
  python3 tests/endurance_5device.py --phase A          # smoke only
  python3 tests/endurance_5device.py --phase D --count 2000
  python3 tests/endurance_5device.py --skip-devices Pager  # skip broken device
  python3 tests/endurance_5device.py --iphone            # add iPhone TCP participant
  python3 tests/endurance_5device.py --iphone-target BPF  # iPhone connects through BPF
"""
import sys, os, time, random, threading, argparse, json, collections, struct, glob, subprocess, re, signal, fcntl, atexit
import meshtastic
import meshtastic.serial_interface
from pubsub import pub

LOCK_FILE = '/tmp/endurance_test.lock'
_lock_fd = None

def acquire_lock():
    """Acquire exclusive lock file. Exit if another test is already running."""
    global _lock_fd
    try:
        _lock_fd = open(LOCK_FILE, 'w')
        fcntl.flock(_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _lock_fd.write(f"{os.getpid()}\n")
        _lock_fd.flush()
        atexit.register(release_lock)
    except (IOError, OSError):
        # Another test holds the lock — read its PID and exit
        try:
            with open(LOCK_FILE) as f:
                other_pid = f.read().strip()
        except Exception:
            other_pid = '?'
        print(f"  *** Another endurance test (PID {other_pid}) is already running. Exiting. ***")
        sys.exit(1)

def release_lock():
    global _lock_fd
    if _lock_fd:
        try:
            fcntl.flock(_lock_fd, fcntl.LOCK_UN)
            _lock_fd.close()
            os.unlink(LOCK_FILE)
        except Exception:
            pass
        _lock_fd = None

def _sig_handler(signum, frame):
    if signum == signal.SIGTERM:
        # Ignore SIGTERM — external process managers send this but
        # we want the test to run until completion or SIGINT (Ctrl-C)
        return
    print(f"\n\nCaught signal {signum} ({signal.Signals(signum).name})", flush=True)
    import traceback
    traceback.print_stack(frame)
    release_lock()
    sys.exit(128 + signum)
for _s in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
    signal.signal(_s, _sig_handler)

# Monkey-patch meshtastic's our_exit() to raise an exception instead of
# calling sys.exit(). The library calls our_exit on things like
# NO_RESPONSE routing acks, which silently kills the test process.
class MeshtasticError(Exception):
    pass

def _patched_our_exit(message, return_value=1):
    raise MeshtasticError(f"meshtastic exit: {message}")

meshtastic.util.our_exit = _patched_our_exit

# ─── hwModel → Device Name & Band Mapping ────────────────────────────────
HWMODEL_MAP = {
    'T_LORA_PAGER':          {'name': 'Pager', 'band': '915'},
    'LILYGO_TBEAM_S3_CORE':  {'name': 'TBeam', 'band': '144'},
    'TBEAM_BPF':             {'name': 'BPF',   'band': '144'},
    '124':                   {'name': 'BPF',   'band': '144'},  # numeric hwModel for BPF
    'TLORA_T3_S3':           {'name': 'T3S3',  'band': '915'},
    'XIAO_ESP32S3':          {'name': 'XIAO',  'band': '915'},
    'SEEED_XIAO_S3':         {'name': 'XIAO',  'band': '915'},
}

# Second TBeam disambiguator: if we see two LILYGO_TBEAM_S3_CORE, the second
# is named TBeam2 (VHF-A vs VHF-B)
KNOWN_TBEAM_IDS = {
    0x335e1bdc: 'TBeam',   # VHF-B
    0x335e1be8: 'TBeam2',  # VHF-A
}

DEVICES = {}  # populated by discover_devices()

CATEGORIES = ['text_dm', 'channel_broadcast', 'image_transfer', 'voice_transfer', 'group_dm']


def discover_devices():
    """Scan all USB serial ports and identify devices by hwModel."""
    ports = sorted(glob.glob('/dev/cu.usbmodem*'))
    if not ports:
        print("  No /dev/cu.usbmodem* ports found!")
        return {}

    devices = {}
    seen_names = set()
    print(f"  Scanning {len(ports)} USB ports...", flush=True)

    for port in ports:
        print(f"    {port}...", end=' ', flush=True)
        try:
            result = subprocess.run(
                ['meshtastic', '--port', port, '--info'],
                capture_output=True, text=True, timeout=20
            )
            output = result.stdout + result.stderr

            # Parse from Metadata line only (not mesh nodes list)
            # Metadata: { ... "hwModel": "T_LORA_PAGER" ... } or "hwModel": 124
            meta_match = re.search(r'Metadata:\s*\{[^}]+\}', output)
            meta_str = meta_match.group(0) if meta_match else ''

            hw_match = re.search(r'"hwModel":\s*"([^"]+)"', meta_str)
            if not hw_match:
                hw_match = re.search(r'"hwModel":\s*(\d+)', meta_str)
            if not hw_match:
                print("no hwModel in Metadata, skipping", flush=True)
                continue
            hw_model = hw_match.group(1)

            # Parse node number from My info line
            num_match = re.search(r'"myNodeNum":\s*(\d+)', output)
            node_num = int(num_match.group(1)) if num_match else 0

            # Parse pioEnv from My info line
            pio_match = re.search(r'"pioEnv":\s*"([^"]+)"', output)
            pio_env = pio_match.group(1) if pio_match else ''

            # Map hwModel to device name (fallback to pioEnv)
            mapping = HWMODEL_MAP.get(hw_model)
            if not mapping and pio_env:
                PIO_FALLBACK = {
                    'tbeam-bpf': {'name': 'BPF', 'band': '144'},
                    'tbeam-s3-core': {'name': 'TBeam', 'band': '144'},
                    'tlora-pager-lr1121': {'name': 'Pager', 'band': '915'},
                    'tlora-t3s3-v1': {'name': 'T3S3', 'band': '915'},
                    'seeed-xiao-s3': {'name': 'XIAO', 'band': '915'},
                }
                mapping = PIO_FALLBACK.get(pio_env)
            if not mapping:
                print(f"unknown hwModel={hw_model} pio={pio_env}, skipping", flush=True)
                continue

            name = mapping['name']
            band = mapping['band']

            # Disambiguate duplicate hwModels (e.g., two TBeams)
            if name in seen_names:
                if node_num in KNOWN_TBEAM_IDS:
                    name = KNOWN_TBEAM_IDS[node_num]
                else:
                    name = f"{name}_{node_num & 0xFFFF:04x}"

            seen_names.add(name)
            devices[name] = {
                'port': port,
                'id': node_num,
                'band': band,
                'hwModel': hw_model,
                'pioEnv': pio_env,
            }
            print(f"{name} (hw={hw_model}, 0x{node_num:08x}, {band}MHz)", flush=True)

        except subprocess.TimeoutExpired:
            print("timeout", flush=True)
        except Exception as e:
            print(f"error: {e}", flush=True)

    return devices


def resolve_device_ip(device_name):
    """Resolve a meshtastic device's WiFi IP via mDNS."""
    try:
        # Browse for _meshtastic._tcp services
        browse = subprocess.run(
            ['dns-sd', '-B', '_meshtastic._tcp', 'local.'],
            capture_output=True, text=True, timeout=6
        )
    except (subprocess.TimeoutExpired, Exception):
        pass

    # Try resolving known mDNS names
    for suffix in ['', '-2', '-3', '-4', '-5', '-6', '-7', '-8', '-9']:
        mdns_name = f"Meshtastic{suffix}"
        try:
            lookup = subprocess.run(
                ['dns-sd', '-L', mdns_name, '_meshtastic._tcp', 'local.'],
                capture_output=True, text=True, timeout=4
            )
            output = lookup.stdout + lookup.stderr
            # Check if this service matches our device (by pio_env or id)
            if device_name in DEVICES:
                dev = DEVICES[device_name]
                node_id = f"!{dev['id']:08x}" if dev.get('id') else ''
                pio = dev.get('pioEnv', '')
                if (pio and pio in output) or (node_id and node_id in output):
                    # Found it — now resolve the IP
                    host_match = re.search(r'can be reached at (\S+):(\d+)', output)
                    if host_match:
                        hostname = host_match.group(1)
                        resolve = subprocess.run(
                            ['dns-sd', '-G', 'v4', hostname],
                            capture_output=True, text=True, timeout=4
                        )
                        ip_match = re.search(r'(\d+\.\d+\.\d+\.\d+)', resolve.stdout + resolve.stderr)
                        if ip_match:
                            return ip_match.group(1)
        except (subprocess.TimeoutExpired, Exception):
            continue
    return None


def build_pairs(devices):
    """Build same-band, cross-band, and all pairs from discovered devices."""
    names = list(devices.keys())
    same_band_pairs = []
    cross_band_pairs = []

    for i, s in enumerate(names):
        for j, d in enumerate(names):
            if i == j:
                continue
            # Skip iPhone↔iPhone
            if devices[s].get('is_tcp') and devices[d].get('is_tcp'):
                continue
            s_band = devices[s]['band']
            d_band = devices[d]['band']
            if s_band == d_band:
                same_band_pairs.append((s, d))
            else:
                cross_band_pairs.append((s, d))

    return same_band_pairs, cross_band_pairs, same_band_pairs + cross_band_pairs

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


def _rc(rate):
    if rate >= 95: return C['g']
    if rate >= 80: return C['y']
    return C['r']


class FiveDeviceEndurance:
    def __init__(self, args):
        self.phase = args.phase.upper()
        self.count = args.count
        self.delay = args.delay
        self.target_rate = args.target_rate
        self.skip_devices = set(s.strip() for s in (args.skip_devices or '').split(',') if s.strip())
        self.add_iphone = args.iphone
        self.iphone_target = args.iphone_target
        self.interfaces = {}
        self.iface_to_name = {}
        self.lock = threading.Lock()
        self.running = True
        self.start_time = None
        self.logfile = f"/tmp/endurance_5dev_{int(time.time())}.jsonl"
        self.learnings_file = f"/tmp/endurance_5dev_learnings_{int(time.time())}.md"
        self.last_status_time = time.time()

        # Stats — initialized after discovery in setup()
        self.rolling = {cat: collections.deque(maxlen=100) for cat in CATEGORIES}
        self.cumulative = {cat: {'sent': 0, 'ok': 0} for cat in CATEGORIES}
        self.pair_stats = {}
        self.pending_rx = {}
        self.pending_ack = {}
        self.signal = {}
        self.learnings = []

        # Pair lists — populated after discovery
        self.same_band_pairs = []
        self.cross_band_pairs = []
        self.all_pairs = []

    # ─── Setup ────────────────────────────────────────────────────────

    def setup(self):
        global DEVICES
        print(f"{'='*70}")
        print(f"  5-DEVICE DUAL-BAND ENDURANCE TEST (Auto-Discovery)")
        print(f"  Phase: {self.phase}, Count: {self.count or 'INFINITE'}, Delay: {self.delay}s")
        print(f"  Target: {self.target_rate}%, Log: {self.logfile}")
        if self.skip_devices: print(f"  Skip: {', '.join(self.skip_devices)}")
        if self.add_iphone: print(f"  iPhone: TCP participant via {self.iphone_target or 'auto'}")
        print(f"{'='*70}\n")

        # Step 0: Acquire exclusive lock — prevents multiple test instances
        acquire_lock()

        # Step 1: Kill stale meshtastic CLI processes (not other test instances)
        print("  Checking for stale meshtastic processes...", flush=True)
        my_pid = str(os.getpid())
        try:
            result = subprocess.run(['lsof'] + glob.glob('/dev/cu.usbmodem*'),
                                    capture_output=True, text=True, timeout=5)
            if result.stdout.strip():
                pids = set()
                for line in result.stdout.strip().split('\n')[1:]:
                    parts = line.split()
                    if len(parts) >= 2:
                        pid = parts[1]
                        cmd = parts[0] if parts else ''
                        # Only kill meshtastic CLI processes, not other Python tests
                        if pid != my_pid and cmd.lower() in ('meshtastic', 'meshtas+'):
                            pids.add(pid)
                for pid in pids:
                    print(f"    Killing stale meshtastic PID {pid}", flush=True)
                    subprocess.run(['kill', pid], capture_output=True, timeout=3)
                if pids:
                    time.sleep(2)
        except Exception:
            pass

        # Step 2: Auto-discover devices
        print("\n  --- Device Discovery ---", flush=True)
        DEVICES = discover_devices()

        # Apply skip list
        for skip in self.skip_devices:
            if skip in DEVICES:
                print(f"  Skipping {skip} (user request)", flush=True)
                del DEVICES[skip]

        if len(DEVICES) < 2:
            print(f"  *** Found {len(DEVICES)} devices, need >= 2 ***")
            return False

        # Step 3: Build pair lists from discovered devices
        self.same_band_pairs, self.cross_band_pairs, self.all_pairs = build_pairs(DEVICES)

        # Step 4: Open serial interfaces
        print(f"\n  --- Opening Interfaces ---", flush=True)
        pub.subscribe(self._on_rx, "meshtastic.receive")
        pub.subscribe(self._on_disconnect, "meshtastic.connection.lost")

        for name, cfg in DEVICES.items():
            if cfg.get('is_tcp'):
                continue  # TCP devices opened separately
            port = cfg['port']
            print(f"  Opening {name} ({port}, {cfg['band']} MHz)...", flush=True)
            try:
                iface = meshtastic.serial_interface.SerialInterface(port)
                time.sleep(3)
                node = iface.getMyNodeInfo()
                num = node.get('num', 0)
                DEVICES[name]['id'] = num
                self.interfaces[name] = iface
                self.iface_to_name[id(iface)] = name
                hw = node.get('user', {}).get('hwModel', '?')
                print(f"    -> 0x{num:08x} hw={hw} band={cfg['band']}MHz", flush=True)
            except Exception as e:
                print(f"    -> FAILED: {e}", flush=True)

        # Step 5: Add iPhone TCP participant if requested
        if self.add_iphone:
            self._setup_iphone_tcp()

        # Rebuild pairs after connections established (some may have failed)
        self.same_band_pairs, self.cross_band_pairs, self.all_pairs = build_pairs(DEVICES)

        # Initialize stats structures
        for s, d in self.all_pairs:
            k = f"{s}->{d}"
            self.pair_stats[k] = {cat: {'sent': 0, 'ok': 0} for cat in CATEGORIES}
            self.signal[k] = collections.deque(maxlen=20)
        for name in DEVICES:
            k = f"{name}->ALL"
            self.pair_stats[k] = {cat: {'sent': 0, 'ok': 0} for cat in CATEGORIES}

        active = [n for n in DEVICES if n in self.interfaces]
        band_915 = [n for n in active if DEVICES[n]['band'] == '915']
        band_144 = [n for n in active if DEVICES[n]['band'] == '144']
        tcp_devs = [n for n in active if DEVICES[n].get('is_tcp')]
        print(f"\n  Active: {len(active)}/{len(DEVICES)} — "
              f"915MHz: {band_915}, 144MHz: {band_144}"
              f"{f', TCP: {tcp_devs}' if tcp_devs else ''}")
        print(f"  Same-band pairs: {len(self.same_band_pairs)}, "
              f"Cross-band pairs: {len(self.cross_band_pairs)}")

        if len(active) < 2:
            print("  *** Need >= 2 active devices ***")
            return False

        print(f"  Settling 10s...", flush=True)
        time.sleep(10)
        print(flush=True)
        return True

    def _setup_iphone_tcp(self):
        """Connect an 'iPhone' TCP participant through a WiFi-enabled device."""
        # Determine target device for TCP connection
        target = self.iphone_target
        if not target:
            # Auto-pick: prefer BPF, then TBeam, then first WiFi device
            for candidate in ['BPF', 'TBeam', 'TBeam2']:
                if candidate in DEVICES and candidate in self.interfaces:
                    target = candidate
                    break
            if not target:
                # Pick any connected device
                for name in self.interfaces:
                    target = name
                    break

        if not target or target not in DEVICES:
            print(f"  iPhone: No target device available for TCP", flush=True)
            return

        print(f"\n  --- iPhone TCP Setup (through {target}) ---", flush=True)

        # Resolve target device IP via mDNS
        ip = resolve_device_ip(target)
        if not ip:
            # Fallback: try direct hostname resolution
            try:
                import socket
                for suffix in ['', '-2', '-3', '-4', '-5', '-6', '-7', '-8', '-9']:
                    try:
                        hostname = f"Meshtastic{suffix}.local"
                        resolved_ip = socket.gethostbyname(hostname)
                        # Verify it's the right device by connecting and checking
                        ip = resolved_ip
                        print(f"    Resolved {hostname} -> {ip}", flush=True)
                        break
                    except socket.gaierror:
                        continue
            except Exception:
                pass

        if not ip:
            print(f"  iPhone: Could not resolve IP for {target}", flush=True)
            return

        print(f"  iPhone: Connecting via TCP to {target} ({ip}:4403)...", flush=True)
        try:
            import meshtastic.tcp_interface
            tcp_iface = meshtastic.tcp_interface.TCPInterface(ip, portNumber=4403)
            time.sleep(3)
            node = tcp_iface.getMyNodeInfo()
            num = node.get('num', 0)

            # The iPhone shares the target device's node ID and radio
            target_band = DEVICES[target]['band']
            DEVICES['iPhone'] = {
                'port': f'tcp:{ip}:4403',
                'id': num,
                'band': target_band,
                'hwModel': 'TCP_CLIENT',
                'pioEnv': '',
                'is_tcp': True,
                'tcp_target': target,
                'tcp_ip': ip,
            }
            self.interfaces['iPhone'] = tcp_iface
            self.iface_to_name[id(tcp_iface)] = 'iPhone'
            print(f"    -> iPhone connected (0x{num:08x}, band={target_band}MHz via {target})", flush=True)

            # NOTE: TCP and serial share the same node, so the iPhone's
            # node ID is the same as the target device. Messages sent via TCP
            # go through the target's radio, same path as a real iPhone.
            # We need to close the serial interface for the target device
            # since TCP port 4403 only accepts ONE connection.
            if target in self.interfaces and self.interfaces[target] is not tcp_iface:
                print(f"    NOTE: {target} serial still open (TCP is a separate conn)", flush=True)

        except Exception as e:
            print(f"    -> iPhone TCP FAILED: {e}", flush=True)
            self._record_learning(f"iPhone TCP connection failed: {e}")

    # ─── Receive Callback ─────────────────────────────────────────────

    def _on_rx(self, packet, interface):
        try:
            self._on_rx_inner(packet, interface)
        except Exception as e:
            ts = time.strftime("%H:%M:%S")
            print(f"  [{ts}] RX callback error: {e}", flush=True)

    def _on_rx_inner(self, packet, interface):
        iface_id = id(interface)
        receiver = self.iface_to_name.get(iface_id, '?')
        decoded = packet.get("decoded", {})
        portnum = decoded.get("portnum", "?")
        payload = decoded.get("payload", b"")
        rssi = packet.get('rxRssi') or packet.get('rssi')
        snr = packet.get('rxSnr') or packet.get('snr')
        from_id = packet.get('fromId', '') or ''

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
                            if info.get('expect_receiver'):
                                if receiver == info['expect_receiver']:
                                    info['event'].set()
                            else:
                                info['event'].set()

        if portnum in (259, "MEDIA_TRANSFER_APP"):
            mtype_str = self._decode_media_type(payload)
            tid_val = self._decode_tid(payload)

            if mtype_str in ("ACK_COMPLETE", "NACK"):
                with self.lock:
                    matched = False
                    if tid_val and tid_val in self.pending_ack:
                        state = self.pending_ack[tid_val]
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

    def test_text_dm(self, src, dst, timeout=15):
        ok, lat = self._send_text_dm_once(src, dst, timeout)
        if not ok:
            # Reconnect BOTH sender and receiver — timeout usually means
            # the receiver's serial dropped, not the sender's
            self._ensure_connected(src)
            self._ensure_connected(dst)
            time.sleep(2)
            ok, lat = self._send_text_dm_once(src, dst, timeout)
            if ok:
                self._record_learning(f"TEXT_DM retry succeeded: {src}->{dst}")
        return ok, lat

    def _send_text_dm_once(self, src, dst, timeout=15):
        iface = self.interfaces.get(src)
        dest_id = DEVICES[dst]['id']
        if not iface or not dest_id:
            return False, None

        tid = f"DM-{src[:2]}{dst[:2]}-{int(time.time())%100000}-{random.randint(100,999)}"
        msg = f"{tid} txt"

        event = threading.Event()
        with self.lock:
            self.pending_rx[tid] = {
                'event': event, 'sender': src, 'expect_receiver': dst,
                'received_by': set(), 'rssi': None, 'snr': None,
            }

        send_time = time.time()
        try:
            self._safe_send_text(iface, msg, destinationId=dest_id, wantAck=True, wantResponse=False)
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
        ok, lat = self._send_broadcast_once(src)
        if not ok:
            # Reconnect sender and all same-band receivers
            self._ensure_connected(src)
            src_band = DEVICES[src]['band']
            for name in DEVICES:
                if name != src and DEVICES[name]['band'] == src_band:
                    self._ensure_connected(name)
            time.sleep(2)
            ok, lat = self._send_broadcast_once(src)
            if ok:
                self._record_learning(f"BROADCAST retry succeeded: {src}->ALL")
        return ok, lat

    def _send_broadcast_once(self, src):
        iface = self.interfaces.get(src)
        if not iface:
            return False, None

        tid = f"CH-{src[:2]}-{int(time.time())%100000}-{random.randint(100,999)}"
        msg = f"{tid} bcast"

        event = threading.Event()
        with self.lock:
            self.pending_rx[tid] = {
                'event': event, 'sender': src, 'expect_receiver': None,
                'received_by': set(), 'rssi': None, 'snr': None,
            }

        send_time = time.time()
        try:
            self._safe_send_text(iface, msg, wantAck=False, wantResponse=False)
        except Exception as e:
            with self.lock: self.pending_rx.pop(tid, None)
            self._reconnect(src)
            return False, None

        got = event.wait(timeout=25)
        with self.lock:
            info = self.pending_rx.pop(tid, {})

        receivers = info.get('received_by', set()) - {src}
        if receivers:
            lat = time.time() - send_time
            return True, lat
        return False, None

    def test_image_transfer(self, src, dst):
        tid, ok, lat = self._send_media(src, dst, media_type=1, size=random.choice([20, 50, 100]))
        if not ok:
            # Reconnect FIRST, then cancel on the live connection
            self._ensure_connected(src)
            self._ensure_connected(dst)
            self._send_cancel(src, dst, tid)
            time.sleep(5)
            _, ok, lat = self._send_media(src, dst, media_type=1, size=random.choice([20, 50]))
            if ok:
                self._record_learning(f"IMAGE retry succeeded: {src}->{dst}")
        return ok, lat

    def test_voice_transfer(self, src, dst):
        tid, ok, lat = self._send_media(src, dst, media_type=2, size=random.choice([20, 50, 80]))
        if not ok:
            # Reconnect FIRST, then cancel on the live connection
            self._ensure_connected(src)
            self._ensure_connected(dst)
            self._send_cancel(src, dst, tid)
            time.sleep(5)
            _, ok, lat = self._send_media(src, dst, media_type=2, size=random.choice([20, 50]))
            if ok:
                self._record_learning(f"VOICE retry succeeded: {src}->{dst}")
        return ok, lat

    def _send_cancel(self, src, dst, tid):
        if not tid: return
        iface = self.interfaces.get(src)
        dest_id = DEVICES[dst]['id']
        if not iface or not dest_id: return
        cancel_pkt = bytearray()
        cancel_pkt.extend(_fv(1, 5))  # type=CANCEL
        cancel_pkt.extend(_fv(2, tid))
        try:
            self._safe_send(iface, bytes(cancel_pkt), destinationId=dest_id, portNum=259,
                            wantAck=False, wantResponse=False)
            time.sleep(3)
            self._safe_send(iface, bytes(cancel_pkt), destinationId=dest_id, portNum=259,
                            wantAck=False, wantResponse=False)
        except: pass
        time.sleep(3)

    def test_group_dm(self, src, destinations):
        iface = self.interfaces.get(src)
        if not iface:
            return False, None

        successes = 0
        send_time = time.time()

        for idx, dst in enumerate(destinations):
            dest_id = DEVICES[dst]['id']
            if not dest_id: continue
            ok = self._send_group_member(src, dst, idx, iface, dest_id)
            if not ok:
                time.sleep(3)
                ok = self._send_group_member(src, dst, idx, iface, dest_id)
                if ok:
                    self._record_learning(f"GROUP_DM retry succeeded: {src}->{dst}")
            if ok: successes += 1
            time.sleep(3)

        total_lat = time.time() - send_time
        return successes == len(destinations), total_lat

    def _send_group_member(self, src, dst, idx, iface, dest_id):
        sub_tid = f"GRP-{src[:2]}-{int(time.time())%100000}-{random.randint(100,999)}-{idx}"
        msg = f"{sub_tid} grp"
        event = threading.Event()
        with self.lock:
            self.pending_rx[sub_tid] = {
                'event': event, 'sender': src, 'expect_receiver': dst,
                'received_by': set(), 'rssi': None, 'snr': None,
            }
        try:
            self._safe_send_text(iface, msg, destinationId=dest_id, wantAck=False, wantResponse=False)
        except:
            with self.lock: self.pending_rx.pop(sub_tid, None)
            return False
        got = event.wait(timeout=20)
        with self.lock:
            info = self.pending_rx.pop(sub_tid, {})
        if got and dst in info.get('received_by', set()):
            self._record_signal(src, dst, info)
            return True
        return False

    def _safe_send(self, iface, *args, **kwargs):
        """sendData with a 15s thread-based timeout to prevent hanging on dead serial."""
        error = [None]
        def do_send():
            try:
                iface.sendData(*args, **kwargs)
            except BaseException as e:
                error[0] = e
        t = threading.Thread(target=do_send, daemon=True)
        t.start()
        t.join(timeout=15)
        if t.is_alive():
            raise TimeoutError("sendData hung (serial likely dead)")
        if error[0]:
            raise Exception(str(error[0]))

    def _safe_send_text(self, iface, *args, **kwargs):
        """sendText with a 15s thread-based timeout to prevent hanging on dead serial."""
        error = [None]
        def do_send():
            try:
                iface.sendText(*args, **kwargs)
            except BaseException as e:
                error[0] = e
        t = threading.Thread(target=do_send, daemon=True)
        t.start()
        t.join(timeout=15)
        if t.is_alive():
            raise TimeoutError("sendText hung (serial likely dead)")
        if error[0]:
            raise Exception(str(error[0]))

    def _send_media(self, src, dst, media_type, size):
        iface = self.interfaces.get(src)
        dest_id = DEVICES[dst]['id']
        if not iface or not dest_id:
            return None, False, None

        chunk_size = 200
        data = bytes([(i * 37 + random.randint(0, 255)) & 0xFF for i in range(size)])
        checksum = _crc32(data)
        tid = random.randint(0x10000, 0xFFFFFFF)
        total_chunks = (len(data) + chunk_size - 1) // chunk_size

        def build_start():
            pkt = bytearray()
            pkt.extend(_fv(1, 1))       # type = START
            pkt.extend(_fv(2, tid))
            pkt.extend(_fv(4, total_chunks))
            pkt.extend(_fv(5, size))
            pkt.extend(_fv(7, media_type))
            pkt.extend(_fv(9, checksum))
            return bytes(pkt)

        def build_chunk(idx):
            offset = idx * chunk_size
            end = min(offset + chunk_size, len(data))
            pkt = bytearray()
            pkt.extend(_fv(2, tid))
            if idx > 0:
                pkt.extend(_fv(3, idx))  # chunk_index (field 3)
            pkt.extend(_fb(6, data[offset:end]))
            return bytes(pkt)

        def build_complete():
            pkt = bytearray()
            pkt.extend(_fv(1, 2))       # type = COMPLETE
            pkt.extend(_fv(2, tid))
            pkt.extend(_fv(9, checksum))
            return bytes(pkt)

        def send_pkt(pkt_bytes):
            self._safe_send(iface, pkt_bytes, destinationId=dest_id, portNum=259,
                            wantAck=False, wantResponse=False)

        t0 = time.time()
        with self.lock:
            self.pending_ack[tid] = {
                'event': threading.Event(), 'result': None,
                'time': t0, 'rx_time': None,
                'rssi': None, 'snr': None,
            }

        MAX_START_RETRIES = 3  # retry entire START+CHUNK+COMPLETE on timeout
        MAX_NACK_RETRIES = 3   # retry chunks on NACK

        for attempt in range(1 + MAX_START_RETRIES):
            try:
                send_pkt(build_start())
                time.sleep(3)
                for ci in range(total_chunks):
                    send_pkt(build_chunk(ci))
                    if ci < total_chunks - 1:
                        time.sleep(3)
                time.sleep(3)
                send_pkt(build_complete())
            except Exception:
                with self.lock: self.pending_ack.pop(tid, None)
                self._reconnect(src)
                return tid, False, None

            # Wait for ACK_COMPLETE or NACK, with NACK retransmission loop
            for nack_round in range(1 + MAX_NACK_RETRIES):
                with self.lock:
                    state = self.pending_ack.get(tid)
                    if state:
                        state['event'] = threading.Event()
                        state['result'] = None
                        evt = state['event']
                    else:
                        break

                got = evt.wait(timeout=30)

                with self.lock:
                    state = self.pending_ack.get(tid)

                if state and state['result'] == 'ACK_COMPLETE':
                    with self.lock: self.pending_ack.pop(tid, None)
                    lat = state['rx_time'] - t0
                    self._record_signal(src, dst, state)
                    return tid, True, lat

                if state and state['result'] == 'NACK':
                    # Retransmit all chunks + COMPLETE (simple: retransmit everything)
                    try:
                        for ci in range(total_chunks):
                            send_pkt(build_chunk(ci))
                            if ci < total_chunks - 1:
                                time.sleep(3)
                        time.sleep(3)
                        send_pkt(build_complete())
                    except Exception:
                        break
                    continue  # wait for ACK_COMPLETE again

                break  # timeout, no NACK — try full retry

        with self.lock: self.pending_ack.pop(tid, None)
        return tid, False, None

    # ─── Cross-band DM (via MQTT) ────────────────────────────────────

    def test_cross_band_dm(self, src, dst, timeout=60):
        """Text DM across bands — relies on MQTT bridge, so longer timeout."""
        ok, lat = self._send_text_dm_once(src, dst, timeout=timeout)
        if not ok:
            self._ensure_connected(src)
            self._ensure_connected(dst)
            time.sleep(3)
            ok, lat = self._send_text_dm_once(src, dst, timeout=timeout)
            if ok:
                self._record_learning(f"CROSS_BAND_DM retry succeeded: {src}->{dst}")
        return ok, lat

    # ─── Helpers ──────────────────────────────────────────────────────

    def _record_signal(self, src, dst, info):
        pk = f"{src}->{dst}"
        rssi = info.get('rssi')
        snr = info.get('snr')
        if rssi is not None:
            if pk not in self.signal:
                self.signal[pk] = collections.deque(maxlen=20)
            self.signal[pk].append({'rssi': rssi, 'snr': snr, 't': time.time()})

    def _on_disconnect(self, interface):
        iface_id = id(interface)
        name = self.iface_to_name.get(iface_id)
        if name:
            ts = time.strftime("%H:%M:%S")
            is_tcp = DEVICES.get(name, {}).get('is_tcp', False)
            conn_type = "TCP" if is_tcp else "serial"
            print(f"  [{ts}] {name} {conn_type} lost", flush=True)
            self.iface_to_name.pop(iface_id, None)
            self.interfaces.pop(name, None)
            try:
                if hasattr(interface, 'stream') and interface.stream:
                    interface.stream.close()
            except: pass
            try: interface.close()
            except: pass

    def _ensure_connected(self, name):
        if name not in self.interfaces:
            return self._reconnect(name)
        iface = self.interfaces[name]
        try:
            if hasattr(iface, 'isConnected') and not iface.isConnected.is_set():
                return self._reconnect(name)
            if hasattr(iface, 'stream') and hasattr(iface.stream, 'is_open') and not iface.stream.is_open:
                return self._reconnect(name)
            return True
        except:
            return self._reconnect(name)

    def _reconnect(self, name, max_attempts=3):
        cfg = DEVICES.get(name, {})
        is_tcp = cfg.get('is_tcp', False)
        ts = time.strftime("%H:%M:%S")
        print(f"  [{ts}] Reconnecting {name}{'(TCP)' if is_tcp else ''}...", flush=True)
        old = self.interfaces.pop(name, None)
        if old:
            old_id = id(old)
            self.iface_to_name.pop(old_id, None)
            try:
                if hasattr(old, 'stream') and old.stream:
                    old.stream.close()
            except: pass
            try: old.close()
            except: pass
        time.sleep(3)

        for attempt in range(max_attempts):
            try:
                if is_tcp:
                    # TCP reconnection (iPhone)
                    ip = cfg.get('tcp_ip')
                    if not ip:
                        raise ValueError("No TCP IP for reconnection")
                    result_holder = [None, None]
                    def do_tcp():
                        try:
                            import meshtastic.tcp_interface as mti
                            result_holder[0] = mti.TCPInterface(ip, portNumber=4403)
                        except Exception as e:
                            result_holder[1] = e
                    t = threading.Thread(target=do_tcp, daemon=True)
                    t.start()
                    t.join(timeout=15)
                    if t.is_alive():
                        raise TimeoutError(f"TCPInterface({ip}) hung")
                    if result_holder[1]:
                        raise result_holder[1]
                    iface = result_holder[0]
                else:
                    # Serial reconnection
                    port = cfg['port']
                    result_holder = [None, None]
                    def do_connect():
                        try:
                            import meshtastic.serial_interface as msi
                            result_holder[0] = msi.SerialInterface(port)
                        except Exception as e:
                            result_holder[1] = e
                    t = threading.Thread(target=do_connect, daemon=True)
                    t.start()
                    t.join(timeout=20)
                    if t.is_alive():
                        raise TimeoutError(f"SerialInterface({port}) hung")
                    if result_holder[1]:
                        raise result_holder[1]
                    iface = result_holder[0]

                time.sleep(3)
                node = iface.getMyNodeInfo()
                num = node.get('num', 0)
                DEVICES[name]['id'] = num
                self.interfaces[name] = iface
                self.iface_to_name[id(iface)] = name
                ts = time.strftime("%H:%M:%S")
                print(f"  [{ts}] {name} reconnected", flush=True)
                return True
            except Exception as e:
                ts = time.strftime("%H:%M:%S")
                print(f"  [{ts}] {name} attempt {attempt+1}/{max_attempts} failed: {e}", flush=True)
                time.sleep(5)
        ts = time.strftime("%H:%M:%S")
        print(f"  [{ts}] {C['r']}{name} reconnect failed (will retry later){C['0']}", flush=True)
        self._record_learning(f"{name} reconnect failed (attempt {max_attempts})")
        return False

    def _record_learning(self, text):
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        entry = f"- [{ts}] {text}"
        self.learnings.append(entry)
        with open(self.learnings_file, 'a') as f:
            f.write(entry + '\n')

    def _active_devices(self, band=None):
        if band:
            return [n for n in DEVICES if n in self.interfaces and DEVICES[n]['band'] == band]
        return [n for n in DEVICES if n in self.interfaces]

    def _active_pairs(self, pair_list):
        return [(s, d) for s, d in pair_list
                if s in self.interfaces and d in self.interfaces]

    def _pairs_for_band(self, band):
        """Get all same-band pairs for a specific band."""
        return [(s, d) for s, d in self.same_band_pairs
                if DEVICES.get(s, {}).get('band') == band and DEVICES.get(d, {}).get('band') == band]

    # ─── Stats ────────────────────────────────────────────────────────

    def _cat_rolling_rate(self, cat):
        d = self.rolling[cat]
        if not d: return 0.0
        return sum(1 for x in d if x) / len(d) * 100

    def _cat_cum_rate(self, cat):
        c = self.cumulative[cat]
        return (c['ok'] / c['sent'] * 100) if c['sent'] > 0 else 0.0

    def _maybe_print_status(self, force=False):
        now = time.time()
        if force or now - self.last_status_time >= 120:
            self.last_status_time = now
            self.print_stats()

    def print_stats(self, test_num=None):
        ts = time.strftime("%H:%M:%S")
        elapsed = time.time() - self.start_time if self.start_time else 0
        h = int(elapsed // 3600)
        m = int((elapsed % 3600) // 60)

        print(f"\n{'='*70}")
        print(f"  5-DEVICE ENDURANCE @ {ts} (elapsed: {h}h{m:02d}m)")
        total_sent = sum(self.cumulative[c]['sent'] for c in CATEGORIES)
        total_ok = sum(self.cumulative[c]['ok'] for c in CATEGORIES)
        if test_num:
            print(f"  Test #{test_num}, Total: {total_ok}/{total_sent}")
        print(f"{'='*70}")

        print(f"\n  {'Category':<22s} {'Rolling(100)':<15s} {'Cumulative':<15s} {'Sent':>5s}")
        print(f"  {'-'*60}")
        for cat in CATEGORIES:
            rr = self._cat_rolling_rate(cat)
            cr = self._cat_cum_rate(cat)
            sent = self.cumulative[cat]['sent']
            if sent == 0: continue
            rc = _rc(rr); cc = _rc(cr); r = C['0']
            print(f"  {cat:<22s} {rc}{rr:>6.1f}%{r} ({len(self.rolling[cat]):>3d})  "
                  f"{cc}{cr:>6.1f}%{r}        {sent:>5d}")

        # Per-pair (only pairs with data)
        print(f"\n  Per-pair:")
        for pk in sorted(self.pair_stats.keys()):
            total_s = sum(self.pair_stats[pk][c]['sent'] for c in CATEGORIES)
            total_o = sum(self.pair_stats[pk][c]['ok'] for c in CATEGORIES)
            if total_s == 0: continue
            rate = total_o / total_s * 100
            rc = _rc(rate)
            sig = ""
            if self.signal.get(pk):
                recent = list(self.signal[pk])[-5:]
                rssis = [s['rssi'] for s in recent if s.get('rssi') is not None]
                if rssis: sig = f" rssi={sum(rssis)/len(rssis):.0f}"
            print(f"    {pk:18s}: {rc}{total_o}/{total_s} ({rate:.0f}%){C['0']}{sig}")

        print(f"{'='*70}\n")

    # ─── Record Result ────────────────────────────────────────────────

    def _record(self, cat, pk, ok, lat, test_num):
        ts = time.strftime("%H:%M:%S")
        self.rolling[cat].append(ok)
        self.cumulative[cat]['sent'] += 1
        if ok: self.cumulative[cat]['ok'] += 1
        # Auto-create pair stats if not already present
        if pk not in self.pair_stats:
            self.pair_stats[pk] = {c: {'sent': 0, 'ok': 0} for c in CATEGORIES}
        self.pair_stats[pk][cat]['sent'] += 1
        if ok: self.pair_stats[pk][cat]['ok'] += 1

        entry = {'time': ts, 'epoch': time.time(), 'num': test_num,
                 'category': cat, 'pair': pk, 'ok': ok, 'latency': lat}
        with open(self.logfile, 'a') as f:
            f.write(json.dumps(entry) + '\n')

        status = f"{C['g']}OK{C['0']} {lat:.1f}s" if ok else f"{C['r']}FAIL{C['0']}"
        print(status, flush=True)

        if not ok:
            fail_rate = self._cat_rolling_rate(cat)
            if len(self.rolling[cat]) >= 10 and fail_rate < 80:
                self._record_learning(f"DEGRADATION: {cat} rolling={fail_rate:.0f}% on {pk}")

    # ─── Phase A: Smoke Test ─────────────────────────────────────────

    def run_smoke(self):
        print(f"\n{C['b']}═══ PHASE A: SMOKE TEST ═══{C['0']}\n")
        self.start_time = time.time()
        n = 0
        failures = []

        # 1. Same-band text DMs (all pairs)
        for src, dst in self._active_pairs(self.same_band_pairs):
            self._ensure_connected(src)
            self._ensure_connected(dst)
            n += 1
            pk = f"{src}->{dst}"
            print(f"  [A.{n}] TEXT_DM {pk}...", end=' ', flush=True)
            ok, lat = self.test_text_dm(src, dst)
            self._record('text_dm', pk, ok, lat, n)
            if not ok: failures.append(f"text_dm {pk}")
            time.sleep(self.delay)

        # 2. Channel broadcasts (each device once)
        for dev in self._active_devices():
            self._ensure_connected(dev)
            n += 1
            pk = f"{dev}->ALL"
            print(f"  [A.{n}] BROADCAST {pk}...", end=' ', flush=True)
            ok, lat = self.test_channel_broadcast(dev)
            self._record('channel_broadcast', pk, ok, lat, n)
            if not ok: failures.append(f"broadcast {pk}")
            time.sleep(self.delay + 3)

        # 3. Image transfer (one per band)
        for pairs in [self._active_pairs(self._pairs_for_band('915'))[:1], self._active_pairs(self._pairs_for_band('144'))[:1]]:
            for src, dst in pairs:
                self._ensure_connected(src)
                self._ensure_connected(dst)
                n += 1
                pk = f"{src}->{dst}"
                print(f"  [A.{n}] IMAGE {pk}...", end=' ', flush=True)
                ok, lat = self.test_image_transfer(src, dst)
                self._record('image_transfer', pk, ok, lat, n)
                if not ok: failures.append(f"image {pk}")
                time.sleep(self.delay)

        # 4. Voice transfer (one per band)
        for pairs in [self._active_pairs(self._pairs_for_band('915'))[:1], self._active_pairs(self._pairs_for_band('144'))[:1]]:
            for src, dst in pairs:
                self._ensure_connected(src)
                self._ensure_connected(dst)
                n += 1
                pk = f"{src}->{dst}"
                print(f"  [A.{n}] VOICE {pk}...", end=' ', flush=True)
                ok, lat = self.test_voice_transfer(src, dst)
                self._record('voice_transfer', pk, ok, lat, n)
                if not ok: failures.append(f"voice {pk}")
                time.sleep(self.delay)

        # 5. Group DM (one per band)
        for band in ['915', '144']:
            devs = self._active_devices(band)
            if len(devs) >= 2:
                src = devs[0]
                others = devs[1:]
                self._ensure_connected(src)
                for o in others: self._ensure_connected(o)
                n += 1
                pk = f"{src}->{'+'.join(others)}"
                print(f"  [A.{n}] GROUP {pk}...", end=' ', flush=True)
                ok, lat = self.test_group_dm(src, others)
                self._record('group_dm', f"{src}->{others[0]}", ok, lat, n)
                if not ok: failures.append(f"group {pk}")
                time.sleep(self.delay)

        # 6. Cross-band DM (one pair)
        cb_pairs = self._active_pairs(self.cross_band_pairs)
        if cb_pairs:
            src, dst = cb_pairs[0]
            self._ensure_connected(src)
            self._ensure_connected(dst)
            n += 1
            pk = f"{src}->{dst}"
            print(f"  [A.{n}] CROSS-BAND DM {pk}...", end=' ', flush=True)
            ok, lat = self.test_cross_band_dm(src, dst)
            self._record('text_dm', pk, ok, lat, n)
            if not ok: failures.append(f"cross_band {pk}")
            time.sleep(self.delay)

        self.print_stats(n)

        if failures:
            print(f"\n  {C['r']}SMOKE FAILURES ({len(failures)}):{C['0']}")
            for f in failures:
                print(f"    - {f}")
            print(f"\n  Continuing to Phase B despite failures (for iterative tuning).\n")
        else:
            print(f"\n  {C['g']}SMOKE PASSED — all {n} tests OK{C['0']}\n")

        return n

    # ─── Phase B: Same-Band Endurance ─────────────────────────────────

    def run_same_band_endurance(self, start_n=0, target_count=500):
        print(f"\n{C['b']}═══ PHASE B: SAME-BAND ENDURANCE ({target_count} msgs) ═══{C['0']}\n")
        n = start_n
        pairs = self._active_pairs(self.same_band_pairs)
        active = self._active_devices()

        if not pairs:
            print("  No same-band pairs available!")
            return n

        cat_cycle = ['text_dm', 'image_transfer', 'voice_transfer', 'group_dm', 'channel_broadcast']
        bcast_idx = 0  # separate counter for broadcast rotation

        for i in range(target_count):
            # Refresh pairs/active lists every 25 iterations to pick up reconnected devices
            if i % 25 == 0:
                new_pairs = self._active_pairs(self.same_band_pairs)
                new_active = self._active_devices()
                if new_pairs: pairs = new_pairs
                if new_active: active = new_active

            n += 1
            cat = cat_cycle[i % len(cat_cycle)]
            pair = pairs[(i // len(cat_cycle)) % len(pairs)]
            src, dst = pair
            pk = f"{src}->{dst}"
            ts = time.strftime("%H:%M:%S")

            if not self._ensure_connected(src):
                print(f"  [{ts}] #{n}: SKIP ({src} disconnected)", flush=True)
                time.sleep(self.delay); continue
            if cat != 'channel_broadcast' and not self._ensure_connected(dst):
                print(f"  [{ts}] #{n}: SKIP ({dst} disconnected)", flush=True)
                time.sleep(self.delay); continue

            if cat == 'text_dm':
                print(f"  [{ts}] #{n}: TEXT_DM {pk}...", end=' ', flush=True)
                ok, lat = self.test_text_dm(src, dst)
            elif cat == 'image_transfer':
                print(f"  [{ts}] #{n}: IMAGE {pk}...", end=' ', flush=True)
                ok, lat = self.test_image_transfer(src, dst)
            elif cat == 'voice_transfer':
                print(f"  [{ts}] #{n}: VOICE {pk}...", end=' ', flush=True)
                ok, lat = self.test_voice_transfer(src, dst)
            elif cat == 'group_dm':
                band = DEVICES[src]['band']
                others = [d for d in self._active_devices(band) if d != src][:2]
                if not others: others = [dst]
                pk = f"{src}->{others[0]}"
                print(f"  [{ts}] #{n}: GROUP {src}->{'+'.join(others)}...", end=' ', flush=True)
                ok, lat = self.test_group_dm(src, others)
            else:  # channel_broadcast
                sender = active[bcast_idx % len(active)]
                bcast_idx += 1
                pk = f"{sender}->ALL"
                print(f"  [{ts}] #{n}: BROADCAST {pk}...", end=' ', flush=True)
                ok, lat = self.test_channel_broadcast(sender)

            self._record(cat, pk, ok, lat, n)

            if cat == 'channel_broadcast': time.sleep(5)
            elif cat == 'text_dm' and ok: time.sleep(3)
            time.sleep(self.delay)

            if n % 25 == 0:
                self.print_stats(n)
            self._maybe_print_status()

        self.print_stats(n)
        return n

    # ─── Phase C: Cross-Band MQTT Endurance ───────────────────────────

    def run_cross_band_endurance(self, start_n=0, target_count=200):
        print(f"\n{C['b']}═══ PHASE C: CROSS-BAND MQTT ENDURANCE ({target_count} msgs) ═══{C['0']}\n")
        n = start_n
        pairs = self._active_pairs(self.cross_band_pairs)

        if not pairs:
            print("  No cross-band pairs available (need devices on both bands)!")
            return n

        for i in range(target_count):
            n += 1
            pair = pairs[i % len(pairs)]
            src, dst = pair
            pk = f"{src}->{dst}"
            ts = time.strftime("%H:%M:%S")

            if not self._ensure_connected(src) or not self._ensure_connected(dst):
                print(f"  [{ts}] #{n}: SKIP (device disconnected)", flush=True)
                time.sleep(self.delay); continue

            print(f"  [{ts}] #{n}: CROSS-BAND DM {pk}...", end=' ', flush=True)
            ok, lat = self.test_cross_band_dm(src, dst)
            self._record('text_dm', pk, ok, lat, n)
            time.sleep(self.delay)

            if n % 25 == 0:
                self.print_stats(n)
            self._maybe_print_status()

        self.print_stats(n)
        return n

    # ─── Phase D: Full Mixed Endurance ────────────────────────────────

    def run_full_mixed(self, start_n=0, target_count=1000):
        infinite = target_count <= 0
        label = "INFINITE" if infinite else str(target_count)
        print(f"\n{C['b']}═══ PHASE D: FULL MIXED ENDURANCE ({label} msgs) ═══{C['0']}\n")
        n = start_n
        all_pairs = self._active_pairs(self.same_band_pairs)
        cb_pairs = self._active_pairs(self.cross_band_pairs)
        active = self._active_devices()
        bcast_idx = 0  # separate counter for broadcast rotation
        group_idx = 0  # separate counter for group rotation

        if not all_pairs:
            print("  No pairs available!")
            return n

        # Weight: 60% same-band, 20% cross-band, 20% broadcast/group
        # Cat cycle: text, image, voice, cross_band_dm, broadcast, text, group, text, image, voice
        cat_options = ['text_dm', 'image_transfer', 'voice_transfer',
                       'cross_band', 'channel_broadcast',
                       'text_dm', 'group_dm', 'text_dm',
                       'image_transfer', 'voice_transfer']

        i = 0
        while infinite or i < target_count:
            n += 1
            cat = cat_options[i % len(cat_options)]
            ts = time.strftime("%H:%M:%S")

            # Refresh active device/pair lists periodically and try reconnecting missing devices
            if i % 50 == 0:
                for dname in DEVICES:
                    if dname not in self.skip_devices and dname not in self.interfaces:
                        self._ensure_connected(dname)
                all_pairs = self._active_pairs(self.same_band_pairs)
                cb_pairs = self._active_pairs(self.cross_band_pairs)
                active = self._active_devices()
                if not all_pairs and not active:
                    print(f"  [{ts}] No devices available, waiting 30s...", flush=True)
                    time.sleep(30)
                    i += 1; continue

            # Select pair based on category
            if cat == 'cross_band':
                if not cb_pairs:
                    cat = 'text_dm'  # fallback
                    pair = all_pairs[i % len(all_pairs)] if all_pairs else None
                else:
                    pair = cb_pairs[i % len(cb_pairs)]
                if not pair:
                    i += 1; continue
                src, dst = pair
                pk = f"{src}->{dst}"

                if not self._ensure_connected(src) or not self._ensure_connected(dst):
                    print(f"  [{ts}] #{n}: SKIP (disconnected)", flush=True)
                    time.sleep(self.delay); i += 1; continue

                print(f"  [{ts}] #{n}: XBAND DM {pk}...", end=' ', flush=True)
                ok, lat = self.test_cross_band_dm(src, dst)
                self._record('text_dm', pk, ok, lat, n)

            elif cat == 'channel_broadcast':
                if not active:
                    i += 1; continue
                sender = active[bcast_idx % len(active)]
                bcast_idx += 1
                pk = f"{sender}->ALL"

                if not self._ensure_connected(sender):
                    print(f"  [{ts}] #{n}: SKIP ({sender} disconnected)", flush=True)
                    time.sleep(self.delay); i += 1; continue

                print(f"  [{ts}] #{n}: BCAST {pk}...", end=' ', flush=True)
                ok, lat = self.test_channel_broadcast(sender)
                self._record('channel_broadcast', pk, ok, lat, n)
                time.sleep(5)

            elif cat == 'group_dm':
                if not active:
                    i += 1; continue
                src = active[group_idx % len(active)]
                group_idx += 1
                band = DEVICES[src]['band']
                others = [d for d in self._active_devices(band) if d != src][:2]
                if not others:
                    others = [d for d in active if d != src][:1]
                if not others:
                    time.sleep(self.delay); i += 1; continue
                pk = f"{src}->{others[0]}"

                if not self._ensure_connected(src):
                    print(f"  [{ts}] #{n}: SKIP ({src} disconnected)", flush=True)
                    time.sleep(self.delay); i += 1; continue

                print(f"  [{ts}] #{n}: GROUP {src}->{'+'.join(others)}...", end=' ', flush=True)
                ok, lat = self.test_group_dm(src, others)
                self._record('group_dm', pk, ok, lat, n)

            else:  # text_dm, image_transfer, voice_transfer
                if not all_pairs:
                    i += 1; continue
                pair = all_pairs[i % len(all_pairs)]
                src, dst = pair
                pk = f"{src}->{dst}"

                if not self._ensure_connected(src) or not self._ensure_connected(dst):
                    print(f"  [{ts}] #{n}: SKIP (disconnected)", flush=True)
                    time.sleep(self.delay); i += 1; continue

                if cat == 'text_dm':
                    print(f"  [{ts}] #{n}: DM {pk}...", end=' ', flush=True)
                    ok, lat = self.test_text_dm(src, dst)
                elif cat == 'image_transfer':
                    print(f"  [{ts}] #{n}: IMAGE {pk}...", end=' ', flush=True)
                    ok, lat = self.test_image_transfer(src, dst)
                else:
                    print(f"  [{ts}] #{n}: VOICE {pk}...", end=' ', flush=True)
                    ok, lat = self.test_voice_transfer(src, dst)

                self._record(cat, pk, ok, lat, n)
                if cat == 'text_dm' and ok: time.sleep(3)

            time.sleep(self.delay)
            i += 1

            if n % 25 == 0:
                self.print_stats(n)
            elif n % 10 == 0:
                rates = " | ".join(
                    f"{cat[:6]}:{_rc(self._cat_rolling_rate(cat))}"
                    f"{self._cat_rolling_rate(cat):.0f}%{C['0']}"
                    for cat in CATEGORIES if self.cumulative[cat]['sent'] > 0
                )
                print(f"  #{n} {rates}", flush=True)
            self._maybe_print_status()

        self.print_stats(n)
        return n

    # ─── Main ─────────────────────────────────────────────────────────

    def run(self):
        if not self.setup():
            return

        self.start_time = time.time()
        n = 0

        try:
            if self.phase in ('A', 'ALL'):
                n = self.run_smoke()

            if self.phase in ('B', 'ALL'):
                n = self.run_same_band_endurance(start_n=n, target_count=500)

            if self.phase in ('C', 'ALL'):
                n = self.run_cross_band_endurance(start_n=n, target_count=200)

            if self.phase in ('D', 'ALL'):
                target = self.count  # 0 = infinite
                n = self.run_full_mixed(start_n=n, target_count=target)

        except KeyboardInterrupt:
            print("\n\nInterrupted.", flush=True)
            self._record_learning("Test interrupted by user")
        finally:
            self.running = False
            self.print_stats(n)
            self._save_final(n)

            try: pub.unsubscribe(self._on_rx, "meshtastic.receive")
            except: pass
            try: pub.unsubscribe(self._on_disconnect, "meshtastic.connection.lost")
            except: pass
            for iface in self.interfaces.values():
                try: iface.close()
                except: pass

    def _save_final(self, total_tests):
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

        with open(self.learnings_file, 'a') as f:
            f.write(f"\n## Final Summary\n")
            for cat in CATEGORIES:
                cr = self._cat_cum_rate(cat)
                sent = self.cumulative[cat]['sent']
                if sent > 0:
                    f.write(f"- {cat}: cumulative={cr:.1f}%, sent={sent}\n")
            f.write(f"\n### Per-pair\n")
            for pk in sorted(self.pair_stats.keys()):
                total = sum(s['sent'] for s in self.pair_stats[pk].values())
                ok = sum(s['ok'] for s in self.pair_stats[pk].values())
                if total > 0:
                    f.write(f"- {pk}: {ok}/{total} ({ok/total*100:.0f}%)\n")

        print(f"Results: {self.logfile}")
        print(f"Learnings: {self.learnings_file}")


def main():
    parser = argparse.ArgumentParser(description='5-Device Dual-Band Endurance Test')
    parser.add_argument('--phase', type=str, default='ALL',
                        help='Phase to run: A (smoke), B (same-band), C (cross-band), D (full mixed), ALL (default)')
    parser.add_argument('--count', type=int, default=0,
                        help='Override message count for Phase D (0 = infinite)')
    parser.add_argument('--delay', type=float, default=3,
                        help='Delay between tests in seconds (default: 3)')
    parser.add_argument('--target-rate', type=float, default=98.0,
                        help='Target rolling success rate (default: 98.0)')
    parser.add_argument('--skip-devices', type=str, default='',
                        help='Comma-separated device names to skip')
    parser.add_argument('--iphone', action='store_true',
                        help='Add iPhone as TCP participant (connects through a WiFi device)')
    parser.add_argument('--iphone-target', type=str, default='',
                        help='Device name to route iPhone TCP through (default: auto-pick BPF/TBeam)')
    args = parser.parse_args()
    FiveDeviceEndurance(args).run()


if __name__ == "__main__":
    import faulthandler
    faulthandler.enable()

    # Catch ALL unhandled exceptions in threads
    def _thread_excepthook(args):
        print(f"\nThread exception ({args.thread}): {args.exc_type.__name__}: {args.exc_value}", flush=True)
        import traceback
        traceback.print_exception(args.exc_type, args.exc_value, args.exc_traceback)
    threading.excepthook = _thread_excepthook

    def _unraisable_hook(args):
        print(f"\nUnraisable: {args.exc_type.__name__}: {args.exc_value}", flush=True)
    sys.unraisablehook = _unraisable_hook

    try:
        main()
    except BaseException as e:
        import traceback
        print(f"\n\nFATAL ({type(e).__name__}): {e}", flush=True)
        traceback.print_exc()
        if isinstance(e, (KeyboardInterrupt, SystemExit)):
            sys.exit(1)
        sys.exit(1)
