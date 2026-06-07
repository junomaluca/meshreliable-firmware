#!/usr/bin/env python3
"""4-device stress test: media transfers + text DMs across all device pairs.
Tests VHF-A, VHF-B (T-Beam Supreme), BPF-A, BPF-B (T-Beam BPF) on 144 MHz VHF.

All 4 serial interfaces are opened simultaneously. Each interface handles both
sending and receiving. On send failure, the failing device's interface is
reconnected.

Usage:
  python3 stress_test_4dev.py [--count N] [--delay SECS]
  python3 stress_test_4dev.py --count 0                     # infinite mode
  python3 stress_test_4dev.py --count 0 --skip-senders BPF-B --target-rate 99
"""
import sys, time, random, threading, argparse, json, collections
import meshtastic
import meshtastic.serial_interface
from pubsub import pub

DEVICES = {
    'VHF-A': {'port': '/dev/cu.usbmodem101',  'id': 0x335e1be8},
    'VHF-B': {'port': '/dev/cu.usbmodem1101', 'id': 0x335e1bdc},
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

# ANSI colors for heat map
_COLORS = {
    'green': '\033[92m', 'yellow': '\033[93m', 'red': '\033[91m',
    'dim': '\033[2m', 'reset': '\033[0m', 'bold': '\033[1m',
}

# Protobuf helpers
def enc_varint(v):
    buf = bytearray()
    if v == 0: buf.append(0); return buf
    while v > 0x7F: buf.append((v & 0x7F) | 0x80); v >>= 7
    buf.append(v & 0x7F)
    return buf
def enc_fv(fn, val):
    if val == 0: return bytearray()
    return enc_varint((fn << 3) | 0) + enc_varint(val)
def enc_fb(fn, val):
    if not val: return bytearray()
    return enc_varint((fn << 3) | 2) + enc_varint(len(val)) + bytearray(val)
def crc32(data):
    crc = 0xFFFFFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ 0xEDB88320 if crc & 1 else crc >> 1
    return (~crc) & 0xFFFFFFFF

def decode_protobuf_fields(payload):
    fields = {}
    if not isinstance(payload, (bytes, bytearray)) or len(payload) == 0:
        return fields
    pos = 0
    while pos < len(payload):
        tw = 0; shift = 0
        while pos < len(payload):
            b = payload[pos]; pos += 1; tw |= (b & 0x7F) << shift; shift += 7
            if not (b & 0x80): break
        fn = tw >> 3; wt = tw & 7
        if wt == 0:
            v = 0; shift = 0
            while pos < len(payload):
                b = payload[pos]; pos += 1; v |= (b & 0x7F) << shift; shift += 7
                if not (b & 0x80): break
            fields[fn] = v
        elif wt == 2:
            ln = 0; shift = 0
            while pos < len(payload):
                b = payload[pos]; pos += 1; ln |= (b & 0x7F) << shift; shift += 7
                if not (b & 0x80): break
            fields[fn] = payload[pos:pos+ln]
            pos += ln
        elif wt == 5:
            fields[fn] = payload[pos:pos+4]; pos += 4
        elif wt == 1:
            fields[fn] = payload[pos:pos+8]; pos += 8
        else:
            break
    return fields

def decode_type(payload):
    fields = decode_protobuf_fields(payload)
    t = fields.get(1, None)
    if t is not None:
        return {0:"CHUNK",1:"START",2:"COMPLETE",3:"NACK",4:"ACK_COMPLETE",5:"CANCEL"}.get(t, f"?{t}")
    return "CHUNK"

def decode_transfer_id(payload):
    fields = decode_protobuf_fields(payload)
    return fields.get(2, None)


def _rate_color(rate):
    """Return ANSI color code for a success rate."""
    if rate >= 95: return _COLORS['green']
    if rate >= 80: return _COLORS['yellow']
    return _COLORS['red']


class FourDeviceStressTest:
    def __init__(self, args):
        self.count = args.count  # 0 = infinite
        self.delay = args.delay
        self.target_rate = args.target_rate
        self.skip_senders = set(s.strip() for s in (args.skip_senders or '').split(',') if s.strip())
        self.data_sizes = [20, 50, 100, 150, 200]
        self.interfaces = {}
        self.results = {}
        self.reboots = {}
        self.last_uptime = {}
        self.pending_ack = {}
        self.lock = threading.Lock()
        self.start_time = None
        self.logfile = f"/tmp/stress_test_4dev_{int(time.time())}.jsonl"
        self.failed_senders = set()  # devices that consistently fail to send
        self.health_thread = None
        self.running = True
        self.target_reached = False

        # Rolling stats (last 100 tests)
        self.rolling_results = collections.deque(maxlen=100)
        self.rolling_best = 0.0

        # Per-pair failure weighting for adaptive selection
        self.pair_failures = {}  # pair_key -> failure count

        # RSSI/SNR tracking
        self.signal_data = {}  # pair_key -> [{'rssi': ..., 'snr': ...}, ...]

        # Health events log
        self.health_log = f"/tmp/stress_health_{int(time.time())}.jsonl"

        for src, dst in PAIRS:
            key = f"{src}->{dst}"
            self.results[key] = {
                'media_sent': 0, 'media_ack': 0, 'media_nack': 0, 'media_timeout': 0,
                'text_sent': 0, 'text_delivered': 0, 'text_timeout': 0,
                'latencies': []
            }
            self.pair_failures[key] = 0
            self.signal_data[key] = []

    def on_rx(self, packet, interface):
        decoded = packet.get("decoded", {})
        portnum = decoded.get("portnum", "?")
        payload = decoded.get("payload", b"")

        # Track RSSI/SNR from received packets
        rssi = packet.get('rssi')
        snr = packet.get('snr')
        from_id = packet.get('fromId') or packet.get('from')

        if portnum in (259, "MEDIA_TRANSFER_APP"):
            mtype = decode_type(payload)
            tid_from_payload = decode_transfer_id(payload)

            if mtype in ("ACK_COMPLETE", "NACK"):
                with self.lock:
                    if tid_from_payload and tid_from_payload in self.pending_ack:
                        state = self.pending_ack[tid_from_payload]
                        if not state['event'].is_set():
                            state['result'] = mtype
                            state['rx_time'] = time.time()
                            state['rssi'] = rssi
                            state['snr'] = snr
                            state['event'].set()
                            ts = time.strftime("%H:%M:%S")
                            lat = state['rx_time'] - state['time']
                            sig = f" rssi={rssi} snr={snr}" if rssi else ""
                            print(f"    [{ts}] {mtype} tid=0x{tid_from_payload:08X} "
                                  f"({lat:.1f}s){sig}", flush=True)
                            return

                    for tid, state in self.pending_ack.items():
                        if not state['event'].is_set():
                            state['result'] = mtype
                            state['rx_time'] = time.time()
                            state['rssi'] = rssi
                            state['snr'] = snr
                            state['event'].set()
                            ts = time.strftime("%H:%M:%S")
                            lat = state['rx_time'] - state['time']
                            tid_str = f"0x{tid_from_payload:08X}" if tid_from_payload else "?"
                            print(f"    [{ts}] {mtype} tid={tid_str} ({lat:.1f}s)", flush=True)
                            break

    def reconnect_device(self, name):
        port = DEVICES[name]['port']
        ts = time.strftime("%H:%M:%S")
        print(f"  [{ts}] Reconnecting {name} ({port})...", flush=True)

        old_iface = self.interfaces.pop(name, None)
        if old_iface:
            try: old_iface.close()
            except: pass
        time.sleep(3)

        for attempt in range(3):
            try:
                iface = meshtastic.serial_interface.SerialInterface(port)
                time.sleep(3)
                node = iface.getMyNodeInfo()
                node_num = node.get('num', 0)
                DEVICES[name]['id'] = node_num
                self.interfaces[name] = iface
                ts = time.strftime("%H:%M:%S")
                print(f"  [{ts}] {name} reconnected (0x{node_num:08x})", flush=True)
                self._log_health('reconnect_ok', name)
                return True
            except Exception as e:
                ts = time.strftime("%H:%M:%S")
                print(f"  [{ts}] {name} reconnect {attempt+1}/3 failed: {e}", flush=True)
                time.sleep(5)

        print(f"  *** {name} reconnect FAILED ***", flush=True)
        self._log_health('reconnect_fail', name)
        return False

    def check_reboots(self):
        for name in list(DEVICES.keys()):
            iface = self.interfaces.get(name)
            if not iface: continue
            try:
                node = iface.getMyNodeInfo()
                dm = node.get('deviceMetrics', {})
                uptime = dm.get('uptimeSeconds', None)
                if uptime is not None and self.last_uptime.get(name) is not None:
                    if uptime < self.last_uptime[name] - 10:
                        self.reboots[name] = self.reboots.get(name, 0) + 1
                        ts = time.strftime("%H:%M:%S")
                        print(f"  [{ts}] *** REBOOT: {name} "
                              f"({self.last_uptime[name]}s -> {uptime}s) ***", flush=True)
                        self._log_health('reboot', name, {
                            'old_uptime': self.last_uptime[name], 'new_uptime': uptime})
                self.last_uptime[name] = uptime
            except: pass

    def _health_check_loop(self):
        """Background thread: probe each device every 60s."""
        while self.running:
            time.sleep(60)
            if not self.running:
                break
            for name in list(DEVICES.keys()):
                iface = self.interfaces.get(name)
                if not iface:
                    # Try to reconnect disconnected devices
                    self._log_health('disconnected', name)
                    self.reconnect_device(name)
                    continue
                try:
                    node = iface.getMyNodeInfo()
                    dm = node.get('deviceMetrics', {})
                    self._log_health('health_ok', name, {
                        'uptime': dm.get('uptimeSeconds'),
                        'battery': dm.get('batteryLevel'),
                        'voltage': dm.get('voltage'),
                    })
                except Exception as e:
                    self._log_health('health_fail', name, {'error': str(e)})
                    self.reconnect_device(name)

    def _log_health(self, event_type, device, extra=None):
        entry = {
            'time': time.strftime("%H:%M:%S"),
            'epoch': time.time(),
            'event': event_type,
            'device': device,
        }
        if extra:
            entry.update(extra)
        try:
            with open(self.health_log, 'a') as f:
                f.write(json.dumps(entry) + '\n')
        except Exception:
            pass

    def _select_pair(self, valid_pairs, test_num):
        """Adaptive pair selection: weight weak pairs more heavily."""
        if not valid_pairs:
            return None

        # Every 3rd test, pick the weakest pair; otherwise rotate normally
        if test_num % 3 == 0 and any(self.pair_failures.get(f"{s}->{d}", 0) > 0
                                      for s, d in valid_pairs):
            # Sort by failure count descending, pick most-failed pair
            ranked = sorted(valid_pairs,
                          key=lambda p: self.pair_failures.get(f"{p[0]}->{p[1]}", 0),
                          reverse=True)
            return ranked[0]
        else:
            return valid_pairs[test_num % len(valid_pairs)]

    def send_media_transfer(self, src_name, dst_name, size):
        iface = self.interfaces.get(src_name)
        if not iface:
            return 'IFACE_ERROR', None

        dest_id = DEVICES[dst_name]['id']
        if not dest_id:
            return 'NO_DEST', None

        pair_key = f"{src_name}->{dst_name}"
        data = bytes([(i * 37 + random.randint(0, 255)) & 0xFF for i in range(size)])
        checksum = crc32(data)
        tid = random.randint(0x10000, 0xFFFFFFF)

        start_pkt = bytearray()
        start_pkt.extend(enc_fv(1, 1))
        start_pkt.extend(enc_fv(2, tid))
        start_pkt.extend(enc_fv(4, 1))
        start_pkt.extend(enc_fv(5, size))
        start_pkt.extend(enc_fv(7, 3))  # BINARY_DATA
        start_pkt.extend(enc_fv(9, checksum))

        chunk_pkt = bytearray()
        chunk_pkt.extend(enc_fv(2, tid))
        chunk_pkt.extend(enc_fb(6, data))

        complete_pkt = bytearray()
        complete_pkt.extend(enc_fv(1, 2))
        complete_pkt.extend(enc_fv(2, tid))
        complete_pkt.extend(enc_fv(9, checksum))

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
            time.sleep(10)
            iface.sendData(bytes(chunk_pkt), destinationId=dest_id, portNum=259,
                           wantAck=False, wantResponse=False)
            time.sleep(10)
            iface.sendData(bytes(chunk_pkt), destinationId=dest_id, portNum=259,
                           wantAck=False, wantResponse=False)
            time.sleep(10)
            iface.sendData(bytes(complete_pkt), destinationId=dest_id, portNum=259,
                           wantAck=False, wantResponse=False)
        except Exception as e:
            ts = time.strftime("%H:%M:%S")
            print(f"    [{ts}] *** SEND ERROR ({src_name}): {e} ***", flush=True)
            with self.lock:
                self.pending_ack.pop(tid, None)
            self.results[pair_key]['media_sent'] += 1
            self.results[pair_key]['media_timeout'] += 1
            # Try to reconnect
            self.reconnect_device(src_name)
            return 'SEND_ERROR', None

        got_ack = ack_event.wait(timeout=60)
        with self.lock:
            state = self.pending_ack.pop(tid, None)

        self.results[pair_key]['media_sent'] += 1

        if state and state['result'] == 'ACK_COMPLETE':
            latency = state['rx_time'] - state['time']
            self.results[pair_key]['media_ack'] += 1
            self.results[pair_key]['latencies'].append(latency)
            if state.get('rssi') is not None:
                self.signal_data[pair_key].append({
                    'rssi': state['rssi'], 'snr': state['snr'], 'time': state['rx_time']})
            # Clear from failed senders on success
            self.failed_senders.discard(src_name)
            return 'ACK_COMPLETE', latency
        elif state and state['result'] == 'NACK':
            self.results[pair_key]['media_nack'] += 1
            self.pair_failures[pair_key] = self.pair_failures.get(pair_key, 0) + 1
            return 'NACK', None
        else:
            self.results[pair_key]['media_timeout'] += 1
            self.pair_failures[pair_key] = self.pair_failures.get(pair_key, 0) + 1
            return 'TIMEOUT', None

    def send_text_dm(self, src_name, dst_name, msg):
        iface = self.interfaces.get(src_name)
        if not iface:
            return 'IFACE_ERROR', None

        dest_id = DEVICES[dst_name]['id']
        if not dest_id:
            return 'NO_DEST', None

        pair_key = f"{src_name}->{dst_name}"
        send_time = time.time()

        try:
            iface.sendText(msg, destinationId=dest_id, wantAck=True, wantResponse=False)
            time.sleep(10)
            self.results[pair_key]['text_sent'] += 1
            self.results[pair_key]['text_delivered'] += 1
            latency = time.time() - send_time
            self.results[pair_key]['latencies'].append(latency)
            self.failed_senders.discard(src_name)
            return 'DELIVERED', latency
        except Exception as e:
            ts = time.strftime("%H:%M:%S")
            print(f"    [{ts}] *** TEXT ERROR ({src_name}): {e} ***", flush=True)
            self.results[pair_key]['text_sent'] += 1
            self.results[pair_key]['text_timeout'] += 1
            self.pair_failures[pair_key] = self.pair_failures.get(pair_key, 0) + 1
            self.reconnect_device(src_name)
            return 'SEND_ERROR', None

    def _rolling_rate(self):
        """Calculate rolling success rate over last 100 tests."""
        if not self.rolling_results:
            return 0.0
        successes = sum(1 for r in self.rolling_results if r)
        return successes / len(self.rolling_results) * 100

    def _cumulative_rate(self):
        """Calculate overall cumulative success rate."""
        total_sent = sum(r['media_sent'] + r['text_sent'] for r in self.results.values())
        total_ok = sum(r['media_ack'] + r['text_delivered'] for r in self.results.values())
        return (total_ok / total_sent * 100) if total_sent > 0 else 0.0

    def print_live_status(self, test_num):
        """Compact single-line status between full dumps."""
        rolling = self._rolling_rate()
        cumulative = self._cumulative_rate()
        total_sent = sum(r['media_sent'] + r['text_sent'] for r in self.results.values())
        rc = _rate_color(rolling)
        cc = _rate_color(cumulative)
        reset = _COLORS['reset']
        count_str = f"/{self.count}" if self.count > 0 else ""
        print(f"  #{test_num}{count_str} | rolling(100): {rc}{rolling:.1f}%{reset} | "
              f"cumulative: {cc}{cumulative:.1f}%{reset} | total: {total_sent}", flush=True)

    def print_stats(self, test_num=None):
        ts = time.strftime("%H:%M:%S")
        elapsed = time.time() - self.start_time
        hours = int(elapsed // 3600)
        mins = int((elapsed % 3600) // 60)

        total_media_sent = sum(r['media_sent'] for r in self.results.values())
        total_media_ack = sum(r['media_ack'] for r in self.results.values())
        total_media_nack = sum(r['media_nack'] for r in self.results.values())
        total_media_timeout = sum(r['media_timeout'] for r in self.results.values())
        total_text_sent = sum(r['text_sent'] for r in self.results.values())
        total_text_delivered = sum(r['text_delivered'] for r in self.results.values())
        total_text_timeout = sum(r.get('text_timeout', 0) for r in self.results.values())
        all_lats = [l for r in self.results.values() for l in r['latencies']]

        rolling = self._rolling_rate()
        cumulative = self._cumulative_rate()

        count_str = f"/{self.count}" if self.count > 0 else " (infinite)"

        print(f"\n{'='*70}")
        print(f"  4-DEVICE STRESS TEST @ {ts} (elapsed: {hours}h{mins:02d}m)")
        if test_num:
            print(f"  Test #{test_num}{count_str}")
        print(f"{'='*70}")

        # Rolling stats
        rc = _rate_color(rolling)
        cc = _rate_color(cumulative)
        reset = _COLORS['reset']
        print(f"  ROLLING(100): {rc}{rolling:.1f}%{reset}  |  "
              f"CUMULATIVE: {cc}{cumulative:.1f}%{reset}  |  "
              f"BEST ROLLING: {self.rolling_best:.1f}%")

        if self.target_rate and not self.target_reached and rolling >= self.target_rate:
            self.target_reached = True
            print(f"  {_COLORS['bold']}*** TARGET {self.target_rate}% REACHED! ***{reset}")

        if total_media_sent > 0:
            rate = total_media_ack / total_media_sent * 100
            avg_lat = sum(all_lats) / len(all_lats) if all_lats else 0
            print(f"\n  MEDIA: {total_media_ack}/{total_media_sent} ({rate:.1f}%) "
                  f"NACK={total_media_nack} Timeout={total_media_timeout} "
                  f"Avg={avg_lat:.1f}s")

        if total_text_sent > 0:
            rate = total_text_delivered / total_text_sent * 100
            print(f"  TEXT:  {total_text_delivered}/{total_text_sent} ({rate:.1f}%) "
                  f"Timeout={total_text_timeout}")

        # Per-pair heat map
        active_pairs = {k: r for k, r in self.results.items()
                       if r['media_sent'] + r['text_sent'] > 0}
        if active_pairs:
            print(f"\n  Per-pair breakdown:")
            for pk in sorted(active_pairs.keys()):
                r = active_pairs[pk]
                total = r['media_sent'] + r['text_sent']
                success = r['media_ack'] + r['text_delivered']
                rate = success / total * 100 if total > 0 else 0
                color = _rate_color(rate)
                # Signal quality
                sig_str = ""
                if self.signal_data.get(pk):
                    recent = self.signal_data[pk][-5:]
                    avg_rssi = sum(s['rssi'] for s in recent if s.get('rssi')) / max(1, len([s for s in recent if s.get('rssi')]))
                    avg_snr = sum(s['snr'] for s in recent if s.get('snr')) / max(1, len([s for s in recent if s.get('snr')]))
                    sig_str = f" rssi={avg_rssi:.0f} snr={avg_snr:.1f}"
                print(f"    {pk:15s}: {color}{success}/{total} ({rate:.0f}%){reset} "
                      f"[m={r['media_ack']}/{r['media_sent']} "
                      f"t={r['text_delivered']}/{r['text_sent']}]{sig_str}")

        if self.failed_senders:
            print(f"\n  Failed senders (send-only): {', '.join(sorted(self.failed_senders))}")
        if self.skip_senders:
            print(f"  Skipped senders: {', '.join(sorted(self.skip_senders))}")

        reboots_str = ", ".join(f"{n}={self.reboots.get(n,0)}" for n in DEVICES)
        uptimes_str = ", ".join(f"{n}={self.last_uptime.get(n,'?')}s" for n in DEVICES)
        print(f"\n  REBOOTS: {reboots_str}")
        print(f"  Uptimes: {uptimes_str}")
        print(f"{'='*70}\n")

    def log_result(self, test_num, test_type, pair, result, latency, size=None):
        rolling = self._rolling_rate()
        entry = {
            'time': time.strftime("%H:%M:%S"),
            'epoch': time.time(),
            'num': test_num,
            'type': test_type,
            'pair': pair,
            'result': result,
            'latency': latency,
            'size': size,
            'rolling_rate': round(rolling, 1),
            'reboots': dict(self.reboots),
        }
        with open(self.logfile, 'a') as f:
            f.write(json.dumps(entry) + '\n')

        # Periodic JSON summary every 100 tests
        if test_num % 100 == 0:
            summary = {
                'periodic_summary': True,
                'test_num': test_num,
                'elapsed': time.time() - self.start_time,
                'rolling_rate': round(rolling, 1),
                'cumulative_rate': round(self._cumulative_rate(), 1),
                'results': {k: {kk: vv for kk, vv in v.items() if kk != 'latencies'}
                           for k, v in self.results.items()},
                'reboots': dict(self.reboots),
            }
            with open(self.logfile, 'a') as f:
                f.write(json.dumps(summary) + '\n')

    def run(self):
        self.start_time = time.time()
        pub.subscribe(self.on_rx, "meshtastic.receive")

        infinite = self.count == 0
        count_str = "INFINITE" if infinite else str(self.count)

        print(f"{'='*70}")
        print(f"  4-DEVICE STRESS TEST")
        print(f"  Count: {count_str}, Delay: {self.delay}s")
        if self.skip_senders:
            print(f"  Skip senders: {', '.join(self.skip_senders)}")
        if self.target_rate:
            print(f"  Target rate: {self.target_rate}%")
        print(f"  Log: {self.logfile}")
        print(f"  Health: {self.health_log}")
        print(f"{'='*70}\n")

        # Open all interfaces
        for name in ['VHF-A', 'VHF-B', 'BPF-A', 'BPF-B']:
            if name in self.skip_senders:
                print(f"  Skipping {name} (--skip-senders)", flush=True)
                continue
            port = DEVICES[name]['port']
            print(f"  Opening {name} ({port})...", flush=True)
            try:
                iface = meshtastic.serial_interface.SerialInterface(port)
                time.sleep(3)
                self.interfaces[name] = iface
                node = iface.getMyNodeInfo()
                node_num = node.get('num', 0)
                DEVICES[name]['id'] = node_num
                hw = node.get('user', {}).get('hwModel', '?')
                uptime = node.get('deviceMetrics', {}).get('uptimeSeconds', '?')
                print(f"    -> 0x{node_num:08x} hw={hw} uptime={uptime}s", flush=True)
                self.last_uptime[name] = uptime if isinstance(uptime, int) else None
            except Exception as e:
                print(f"    -> FAILED: {e}", flush=True)

        active = [n for n in DEVICES if n in self.interfaces]
        print(f"\n  Active: {', '.join(active)} ({len(active)}/4)")
        if len(active) < 2:
            print("  *** Need >= 2 devices ***")
            return

        # Start health check thread
        self.health_thread = threading.Thread(target=self._health_check_loop, daemon=True)
        self.health_thread.start()

        print(f"  Waiting 15s for settle...\n", flush=True)
        time.sleep(15)
        self.check_reboots()

        try:
            pair_idx = 0
            send_fail_count = {}  # track consecutive send failures per device
            i = 0
            prev_rolling = 0.0

            while True:
                i += 1
                if not infinite and i > self.count:
                    break

                # Refresh valid pairs, excluding persistent failures and skip list
                excluded = self.failed_senders | self.skip_senders
                valid_pairs = [(s, d) for s, d in PAIRS
                               if s in self.interfaces and d in self.interfaces
                               and s not in excluded and d not in self.skip_senders]

                if not valid_pairs:
                    # Try including failed senders — maybe reconnection worked
                    valid_pairs = [(s, d) for s, d in PAIRS
                                   if s in self.interfaces and d in self.interfaces
                                   and s not in self.skip_senders and d not in self.skip_senders]
                    if not valid_pairs:
                        print("  *** No valid pairs! Trying reconnect... ***")
                        for name in DEVICES:
                            if name not in self.interfaces and name not in self.skip_senders:
                                self.reconnect_device(name)
                        time.sleep(10)
                        continue

                src_name, dst_name = self._select_pair(valid_pairs, i)
                pair_key = f"{src_name}->{dst_name}"

                test_type = 'text' if i % 4 == 0 else 'media'
                ts = time.strftime("%H:%M:%S")

                if test_type == 'media':
                    size = self.data_sizes[(i - 1) % len(self.data_sizes)]
                    print(f"[{ts}] #{i}: MEDIA {pair_key} ({size}B)...", flush=True)
                    result, latency = self.send_media_transfer(src_name, dst_name, size)
                    self.log_result(i, 'media', pair_key, result, latency, size)
                    status = f"OK {latency:.1f}s" if latency else result
                    print(f"  -> {result} ({status})", flush=True)
                    success = result == 'ACK_COMPLETE'
                else:
                    msg = f"4dev#{i} t={int(time.time())} {pair_key}"
                    print(f"[{ts}] #{i}: TEXT {pair_key}...", flush=True)
                    result, latency = self.send_text_dm(src_name, dst_name, msg)
                    self.log_result(i, 'text', pair_key, result, latency, len(msg))
                    status = f"OK {latency:.1f}s" if latency else result
                    print(f"  -> {result} ({status})", flush=True)
                    success = result == 'DELIVERED'

                # Update rolling stats
                self.rolling_results.append(success)
                rolling = self._rolling_rate()
                if rolling > self.rolling_best:
                    self.rolling_best = rolling

                # Degradation detection
                cumulative = self._cumulative_rate()
                if len(self.rolling_results) >= 50 and rolling < cumulative - 5.0:
                    if prev_rolling >= cumulative - 5.0:  # only warn on transition
                        ts2 = time.strftime("%H:%M:%S")
                        print(f"\n  [{ts2}] {_COLORS['red']}*** WARNING: DEGRADATION DETECTED ***{_COLORS['reset']}")
                        print(f"    Rolling={rolling:.1f}% vs Cumulative={cumulative:.1f}% "
                              f"(delta={cumulative-rolling:.1f}%)")
                        print(f"    Pausing 30s for recovery...\n", flush=True)
                        time.sleep(30)
                prev_rolling = rolling

                # Track consecutive send failures
                if result in ('SEND_ERROR', 'IFACE_ERROR'):
                    send_fail_count[src_name] = send_fail_count.get(src_name, 0) + 1
                    if send_fail_count[src_name] >= 3:
                        self.failed_senders.add(src_name)
                        ts2 = time.strftime("%H:%M:%S")
                        print(f"  [{ts2}] *** {src_name} marked as failed sender "
                              f"(3 consecutive failures) ***", flush=True)
                else:
                    send_fail_count[src_name] = 0

                if i % 5 == 0:
                    self.check_reboots()
                if i % 12 == 0:
                    self.print_stats(i)
                elif i % 3 == 0:
                    self.print_live_status(i)

                time.sleep(self.delay)

        except KeyboardInterrupt:
            print("\n\nTest interrupted.", flush=True)
        finally:
            self.running = False
            self.check_reboots()
            self.print_stats()

            final = {
                'summary': True,
                'total_time': time.time() - self.start_time,
                'results': {k: {kk: vv for kk, vv in v.items() if kk != 'latencies'}
                           for k, v in self.results.items()},
                'reboots': self.reboots,
                'failed_senders': list(self.failed_senders),
                'rolling_best': self.rolling_best,
            }
            with open(self.logfile, 'a') as f:
                f.write(json.dumps(final) + '\n')
            print(f"Results saved to {self.logfile}")

            try: pub.unsubscribe(self.on_rx, "meshtastic.receive")
            except: pass
            for iface in self.interfaces.values():
                try: iface.close()
                except: pass


def main():
    parser = argparse.ArgumentParser(description='4-device stress test')
    parser.add_argument('--count', type=int, default=500,
                        help='Number of tests (0 = infinite, default: 500)')
    parser.add_argument('--delay', type=float, default=5,
                        help='Delay between tests (default: 5)')
    parser.add_argument('--skip-senders', type=str, default='',
                        help='Comma-separated device names to exclude as senders (e.g. BPF-B)')
    parser.add_argument('--target-rate', type=float, default=None,
                        help='Target rolling success rate (prints milestone when reached)')
    args = parser.parse_args()

    test = FourDeviceStressTest(args)
    test.run()

if __name__ == "__main__":
    main()
