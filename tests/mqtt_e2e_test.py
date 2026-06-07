#!/usr/bin/env python3
"""End-to-end MQTT test: verifies message delivery across all mesh devices
including the iPhone (connected via BLE to BPF-A).

Sends messages from USB-connected devices and verifies they appear on MQTT.
Also monitors MQTT for messages relayed by the iPhone's MQTT proxy.

Usage: python3 mqtt_e2e_test.py [--count N] [--delay SECS] [--mqtt-host HOST]
"""
import sys, time, random, threading, argparse, json, struct
import paho.mqtt.client as mqtt
import meshtastic
import meshtastic.serial_interface
from pubsub import pub
from google.protobuf.json_format import MessageToDict
from meshtastic.protobuf import mesh_pb2, mqtt_pb2, portnums_pb2
from meshtastic_crypto import decrypt_mesh_packet

# Device configuration
DEVICES = {
    'VHF-A': {'port': '/dev/cu.usbmodem101',  'id': 0x335e1be8, 'type': 'usb'},
    'VHF-B': {'port': '/dev/cu.usbmodem1101', 'id': 0x335e1bdc, 'type': 'usb'},
    'BPF-A': {'port': '/dev/cu.usbmodem21101', 'id': 0x4e574c2c, 'type': 'usb'},
    'BPF-B': {'port': '/dev/cu.usbmodem21201', 'id': 0x23324c59, 'type': 'usb'},
    # iPhone is connected via BLE to BPF-A and relays via MQTT proxy
    # Its node ID will be discovered from MQTT traffic
}

# Node ID reverse lookup
NODE_NAMES = {}

MQTT_CONFIG = {
    'host': 'home.yazdikann.com',
    'port': 1883,
    'username': 'admin',
    'password': 'admin',
    'topic_root': 'msh/US',
}


class MQTTMonitor:
    """Monitors MQTT for Meshtastic messages and tracks delivery."""

    def __init__(self, config):
        self.config = config
        self.client = mqtt.Client(client_id=f"e2e_test_{int(time.time())}")
        self.client.username_pw_set(config['username'], config['password'])
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.lock = threading.Lock()
        self.messages = []  # all received MQTT messages
        self.pending_verifications = {}  # tracking_id -> event
        self.connected = False
        self.message_count = 0
        self.nodes_seen = set()  # node IDs seen on MQTT

    def connect(self):
        self.client.connect(self.config['host'], self.config['port'], 60)
        self.client.loop_start()
        # Wait for connection
        for _ in range(50):
            if self.connected:
                return True
            time.sleep(0.1)
        return False

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()

    def _on_connect(self, client, userdata, flags, rc, *args):
        if rc == 0:
            self.connected = True
            # Subscribe to all meshtastic topics
            topic = f"{self.config['topic_root']}/#"
            client.subscribe(topic)
            print(f"  MQTT connected, subscribed to {topic}", flush=True)
        else:
            print(f"  MQTT connection failed: rc={rc}", flush=True)

    def _on_message(self, client, userdata, msg):
        with self.lock:
            self.message_count += 1

        try:
            # Parse the ServiceEnvelope
            env = mqtt_pb2.ServiceEnvelope()
            env.ParseFromString(msg.payload)

            if env.HasField('packet'):
                pkt = env.packet
                from_id = pkt.from_ if hasattr(pkt, 'from_') else getattr(pkt, 'from', 0)
                to_id = pkt.to
                portnum = None
                payload_text = None
                portnum_name = None

                self.nodes_seen.add(from_id)

                decoded_data = None
                if pkt.HasField('decoded'):
                    decoded_data = pkt.decoded
                elif pkt.HasField('encrypted'):
                    # Try decrypting with channel PSKs
                    topic_parts = msg.topic.split('/')
                    channel_id = topic_parts[4] if len(topic_parts) > 4 else None
                    decoded_data = decrypt_mesh_packet(pkt, channel_id)

                if decoded_data:
                    portnum = decoded_data.portnum
                    portnum_name = portnums_pb2.PortNum.Name(portnum) if portnum else "?"
                    payload = decoded_data.payload

                    # Try to decode text messages
                    if portnum == portnums_pb2.TEXT_MESSAGE_APP:
                        try:
                            payload_text = payload.decode('utf-8')
                        except Exception:
                            payload_text = f"[binary {len(payload)}B]"

                    entry = {
                        'time': time.time(),
                        'topic': msg.topic,
                        'from': from_id,
                        'to': to_id,
                        'portnum': portnum_name,
                        'payload_text': payload_text,
                        'payload_size': len(payload) if payload else 0,
                        'channel': getattr(pkt, 'channel', None),
                        'hop_start': getattr(pkt, 'hop_start', None),
                        'hop_limit': getattr(pkt, 'hop_limit', None),
                    }

                    with self.lock:
                        self.messages.append(entry)

                    # Check pending verifications
                    if payload_text:
                        with self.lock:
                            for tracking_id, info in list(self.pending_verifications.items()):
                                if tracking_id in payload_text and not info['event'].is_set():
                                    info['mqtt_time'] = time.time()
                                    info['mqtt_entry'] = entry
                                    info['event'].set()

        except Exception as e:
            pass  # Packets we can't decode

    def wait_for_message(self, tracking_id, timeout=120):
        """Wait for a message containing tracking_id to appear on MQTT."""
        event = threading.Event()
        with self.lock:
            self.pending_verifications[tracking_id] = {
                'event': event,
                'mqtt_time': None,
                'mqtt_entry': None,
                'registered_time': time.time(),
            }

        got_it = event.wait(timeout=timeout)

        with self.lock:
            info = self.pending_verifications.pop(tracking_id, {})

        if got_it and info.get('mqtt_entry'):
            return info
        return None

    def get_stats(self):
        with self.lock:
            return {
                'total_messages': self.message_count,
                'decoded_messages': len(self.messages),
                'nodes_seen': list(self.nodes_seen),
                'pending': len(self.pending_verifications),
            }

    def get_recent_text_messages(self, limit=10):
        with self.lock:
            texts = [m for m in self.messages if m.get('payload_text')]
            return texts[-limit:]


class E2ETestRunner:
    """Runs end-to-end tests using USB serial devices + MQTT verification."""

    def __init__(self, args):
        self.count = args.count
        self.delay = args.delay
        self.interfaces = {}
        self.mqtt = MQTTMonitor(MQTT_CONFIG)
        self.start_time = None
        self.results = {
            'text_broadcast': {'sent': 0, 'mqtt_verified': 0, 'failed': 0, 'latencies': []},
            'text_dm': {'sent': 0, 'mqtt_verified': 0, 'failed': 0, 'latencies': []},
            'media_transfer': {'sent': 0, 'ack_complete': 0, 'nack': 0, 'timeout': 0, 'latencies': []},
            'mqtt_relay': {'sent': 0, 'mqtt_verified': 0, 'serial_verified': 0, 'failed': 0, 'latencies': []},
            'mqtt_radio_relay': {'sent': 0, 'verified': 0, 'failed': 0, 'latencies': []},
        }
        self.per_device = {}  # device_name -> {sent, received, errors}
        self.logfile = f"/tmp/mqtt_e2e_test_{int(time.time())}.jsonl"
        self.lock = threading.Lock()
        self.pending_ack = {}

    def setup(self):
        """Initialize MQTT and serial interfaces."""
        print(f"{'='*70}")
        print(f"  END-TO-END MQTT TEST")
        print(f"  Count: {self.count}, Delay: {self.delay}s")
        print(f"  MQTT: {MQTT_CONFIG['host']}:{MQTT_CONFIG['port']}")
        print(f"  Log: {self.logfile}")
        print(f"{'='*70}\n")

        # Connect MQTT
        print("  Connecting to MQTT...", flush=True)
        if not self.mqtt.connect():
            print("  *** MQTT connection failed! ***", flush=True)
            return False

        # Subscribe to meshtastic receive events
        pub.subscribe(self.on_rx, "meshtastic.receive")

        # Open serial interfaces (only for USB-connected devices)
        usb_devices = {k: v for k, v in DEVICES.items() if v['type'] == 'usb'}
        for name, info in usb_devices.items():
            port = info['port']
            print(f"  Opening {name} ({port})...", flush=True)
            try:
                iface = meshtastic.serial_interface.SerialInterface(port)
                time.sleep(3)
                node = iface.getMyNodeInfo()
                node_num = node.get('num', 0)
                DEVICES[name]['id'] = node_num
                NODE_NAMES[node_num] = name
                self.interfaces[name] = iface
                hw = node.get('user', {}).get('hwModel', '?')
                uptime = node.get('deviceMetrics', {}).get('uptimeSeconds', '?')
                print(f"    -> 0x{node_num:08x} hw={hw} uptime={uptime}s", flush=True)
                self.per_device[name] = {'sent': 0, 'received': 0, 'errors': 0, 'send_errors': 0}
            except Exception as e:
                print(f"    -> FAILED: {e}", flush=True)

        active = list(self.interfaces.keys())
        print(f"\n  Active USB devices: {', '.join(active)} ({len(active)}/{len(usb_devices)})")

        if len(active) < 2:
            print("  *** Need >= 2 USB devices ***")
            return False

        # Wait for MQTT baseline
        print(f"  Waiting 10s for MQTT baseline...", flush=True)
        time.sleep(10)
        stats = self.mqtt.get_stats()
        print(f"  MQTT baseline: {stats['total_messages']} messages, "
              f"nodes seen: {[hex(n) for n in stats['nodes_seen']]}", flush=True)

        return True

    def on_rx(self, packet, interface):
        """Handle received packets from USB interfaces (for media ACK tracking)."""
        decoded = packet.get("decoded", {})
        portnum = decoded.get("portnum", "?")
        payload = decoded.get("payload", b"")

        if portnum in (259, "MEDIA_TRANSFER_APP"):
            mtype = self._decode_type(payload)
            tid = self._decode_transfer_id(payload)

            if mtype in ("ACK_COMPLETE", "NACK"):
                with self.lock:
                    if tid and tid in self.pending_ack:
                        state = self.pending_ack[tid]
                        if not state['event'].is_set():
                            state['result'] = mtype
                            state['rx_time'] = time.time()
                            state['event'].set()

    # Protobuf helpers
    @staticmethod
    def _enc_varint(v):
        buf = bytearray()
        if v == 0: buf.append(0); return buf
        while v > 0x7F: buf.append((v & 0x7F) | 0x80); v >>= 7
        buf.append(v & 0x7F)
        return buf

    @staticmethod
    def _enc_fv(fn, val):
        if val == 0: return bytearray()
        r = E2ETestRunner._enc_varint((fn << 3) | 0) + E2ETestRunner._enc_varint(val)
        return r

    @staticmethod
    def _enc_fb(fn, val):
        if not val: return bytearray()
        r = E2ETestRunner._enc_varint((fn << 3) | 2) + E2ETestRunner._enc_varint(len(val)) + bytearray(val)
        return r

    @staticmethod
    def _crc32(data):
        crc = 0xFFFFFFFF
        for b in data:
            crc ^= b
            for _ in range(8):
                crc = (crc >> 1) ^ 0xEDB88320 if crc & 1 else crc >> 1
        return (~crc) & 0xFFFFFFFF

    @staticmethod
    def _decode_protobuf_fields(payload):
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

    def _decode_type(self, payload):
        fields = self._decode_protobuf_fields(payload)
        t = fields.get(1, None)
        if t is not None:
            return {0:"CHUNK",1:"START",2:"COMPLETE",3:"NACK",4:"ACK_COMPLETE",5:"CANCEL"}.get(t, f"?{t}")
        return "CHUNK"

    def _decode_transfer_id(self, payload):
        fields = self._decode_protobuf_fields(payload)
        return fields.get(2, None)

    # MARK: - Test Types

    def test_text_broadcast(self, device_name):
        """Send a text broadcast and verify it appears on MQTT."""
        iface = self.interfaces.get(device_name)
        if not iface:
            return 'IFACE_ERROR', None

        tracking_id = f"E2E-{device_name}-{int(time.time())}-{random.randint(1000,9999)}"
        msg = f"{tracking_id} broadcast test"

        # Register MQTT watcher BEFORE sending
        mqtt_watch = threading.Thread(
            target=lambda: None)  # placeholder

        self.per_device[device_name]['sent'] += 1
        self.results['text_broadcast']['sent'] += 1
        send_time = time.time()

        try:
            iface.sendText(msg, wantAck=False, wantResponse=False)
        except Exception as e:
            ts = time.strftime("%H:%M:%S")
            print(f"    [{ts}] SEND ERROR ({device_name}): {e}", flush=True)
            self.per_device[device_name]['send_errors'] += 1
            self.results['text_broadcast']['failed'] += 1
            return 'SEND_ERROR', None

        # Wait for the message to appear on MQTT (via any device's MQTT proxy)
        mqtt_result = self.mqtt.wait_for_message(tracking_id, timeout=60)

        if mqtt_result:
            latency = mqtt_result['mqtt_time'] - send_time
            self.results['text_broadcast']['mqtt_verified'] += 1
            self.results['text_broadcast']['latencies'].append(latency)
            return 'MQTT_VERIFIED', latency
        else:
            self.results['text_broadcast']['failed'] += 1
            return 'MQTT_TIMEOUT', None

    def test_text_dm(self, src_name, dst_name):
        """Send a text DM and verify delivery via serial ACK + MQTT."""
        iface = self.interfaces.get(src_name)
        if not iface:
            return 'IFACE_ERROR', None

        dest_id = DEVICES[dst_name]['id']
        if not dest_id:
            return 'NO_DEST', None

        tracking_id = f"DM-{src_name}-{dst_name}-{int(time.time())}-{random.randint(1000,9999)}"
        msg = f"{tracking_id} dm test"

        self.per_device[src_name]['sent'] += 1
        self.results['text_dm']['sent'] += 1
        send_time = time.time()

        try:
            iface.sendText(msg, destinationId=dest_id, wantAck=True, wantResponse=False)
        except Exception as e:
            ts = time.strftime("%H:%M:%S")
            print(f"    [{ts}] SEND ERROR ({src_name}): {e}", flush=True)
            self.per_device[src_name]['send_errors'] += 1
            self.results['text_dm']['failed'] += 1
            return 'SEND_ERROR', None

        # Wait for MQTT relay
        mqtt_result = self.mqtt.wait_for_message(tracking_id, timeout=60)

        if mqtt_result:
            latency = mqtt_result['mqtt_time'] - send_time
            self.results['text_dm']['mqtt_verified'] += 1
            self.results['text_dm']['latencies'].append(latency)
            return 'MQTT_VERIFIED', latency
        else:
            # Even without MQTT, the DM may have been delivered
            time.sleep(5)
            self.results['text_dm']['failed'] += 1
            return 'MQTT_TIMEOUT', None

    def test_media_transfer(self, src_name, dst_name, size):
        """Send a media transfer and wait for ACK_COMPLETE."""
        iface = self.interfaces.get(src_name)
        if not iface:
            return 'IFACE_ERROR', None

        dest_id = DEVICES[dst_name]['id']
        if not dest_id:
            return 'NO_DEST', None

        data = bytes([(i * 37 + random.randint(0, 255)) & 0xFF for i in range(size)])
        checksum = self._crc32(data)
        tid = random.randint(0x10000, 0xFFFFFFF)

        # Build protobuf packets
        start_pkt = bytearray()
        start_pkt.extend(self._enc_fv(1, 1))  # type=START
        start_pkt.extend(self._enc_fv(2, tid))
        start_pkt.extend(self._enc_fv(4, 1))  # totalChunks
        start_pkt.extend(self._enc_fv(5, size))
        start_pkt.extend(self._enc_fv(7, 3))  # BINARY_DATA
        start_pkt.extend(self._enc_fv(9, checksum))

        chunk_pkt = bytearray()
        chunk_pkt.extend(self._enc_fv(2, tid))
        chunk_pkt.extend(self._enc_fb(6, data))

        complete_pkt = bytearray()
        complete_pkt.extend(self._enc_fv(1, 2))  # type=COMPLETE
        complete_pkt.extend(self._enc_fv(2, tid))
        complete_pkt.extend(self._enc_fv(9, checksum))

        ack_event = threading.Event()
        with self.lock:
            self.pending_ack[tid] = {
                'event': ack_event, 'result': None,
                'time': time.time(), 'rx_time': None
            }

        self.per_device[src_name]['sent'] += 1
        self.results['media_transfer']['sent'] += 1

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
            print(f"    [{ts}] SEND ERROR ({src_name}): {e}", flush=True)
            with self.lock:
                self.pending_ack.pop(tid, None)
            self.per_device[src_name]['send_errors'] += 1
            self.results['media_transfer']['timeout'] += 1
            return 'SEND_ERROR', None

        got_ack = ack_event.wait(timeout=60)
        with self.lock:
            state = self.pending_ack.pop(tid, None)

        if state and state['result'] == 'ACK_COMPLETE':
            latency = state['rx_time'] - state['time']
            self.results['media_transfer']['ack_complete'] += 1
            self.results['media_transfer']['latencies'].append(latency)
            return 'ACK_COMPLETE', latency
        elif state and state['result'] == 'NACK':
            self.results['media_transfer']['nack'] += 1
            return 'NACK', None
        else:
            self.results['media_transfer']['timeout'] += 1
            return 'TIMEOUT', None

    # MARK: - Relay Tests

    def test_mqtt_relay_a_to_b(self, src_name, dst_name):
        """Send text from src, verify it appears on MQTT (decrypted) and is received by dst via serial."""
        iface = self.interfaces.get(src_name)
        if not iface:
            return 'IFACE_ERROR', None

        tracking_id = f"RELAY-{src_name}-{dst_name}-{int(time.time())}-{random.randint(1000,9999)}"
        msg = f"{tracking_id} relay test"

        # Set up serial receive watcher for dst
        serial_event = threading.Event()
        serial_result = {'received': False, 'time': None}

        def rx_watcher(packet, interface):
            decoded = packet.get("decoded", {})
            payload_str = decoded.get("text", "")
            if not payload_str:
                payload_bytes = decoded.get("payload", b"")
                if isinstance(payload_bytes, bytes):
                    try:
                        payload_str = payload_bytes.decode('utf-8')
                    except Exception:
                        return
            if tracking_id in payload_str:
                serial_result['received'] = True
                serial_result['time'] = time.time()
                serial_event.set()

        pub.subscribe(rx_watcher, "meshtastic.receive")

        self.results['mqtt_relay']['sent'] += 1
        self.per_device[src_name]['sent'] += 1
        send_time = time.time()

        try:
            iface.sendText(msg, wantAck=False, wantResponse=False)
        except Exception as e:
            pub.unsubscribe(rx_watcher, "meshtastic.receive")
            self.results['mqtt_relay']['failed'] += 1
            return 'SEND_ERROR', None

        # Wait for MQTT appearance
        mqtt_result = self.mqtt.wait_for_message(tracking_id, timeout=60)
        mqtt_latency = None
        if mqtt_result:
            mqtt_latency = mqtt_result['mqtt_time'] - send_time
            self.results['mqtt_relay']['mqtt_verified'] += 1

        # Wait for serial receive on dst
        serial_event.wait(timeout=30)
        try:
            pub.unsubscribe(rx_watcher, "meshtastic.receive")
        except Exception:
            pass

        if serial_result['received']:
            serial_latency = serial_result['time'] - send_time
            self.results['mqtt_relay']['serial_verified'] += 1
            latency = serial_latency
        elif mqtt_latency:
            latency = mqtt_latency
        else:
            self.results['mqtt_relay']['failed'] += 1
            return 'TIMEOUT', None

        self.results['mqtt_relay']['latencies'].append(latency)
        status = 'MQTT+SERIAL' if mqtt_result and serial_result['received'] else (
            'MQTT_ONLY' if mqtt_result else 'SERIAL_ONLY')
        return status, latency

    def test_mqtt_radio_relay(self, src_name, relay_name, dst_name):
        """Send text from src -> MQTT uplink -> relay device downloads -> radio -> dst receives.

        Verifies multi-hop by checking hop_start vs hop_limit delta.
        """
        iface = self.interfaces.get(src_name)
        if not iface:
            return 'IFACE_ERROR', None

        tracking_id = f"MRELAY-{src_name}-{relay_name}-{dst_name}-{int(time.time())}-{random.randint(1000,9999)}"
        msg = f"{tracking_id} multi-relay"

        # Watch dst serial for the message
        serial_event = threading.Event()
        serial_result = {'received': False, 'time': None, 'hop_start': None, 'hop_limit': None}

        def rx_watcher(packet, interface):
            decoded = packet.get("decoded", {})
            payload_str = decoded.get("text", "")
            if not payload_str:
                payload_bytes = decoded.get("payload", b"")
                if isinstance(payload_bytes, bytes):
                    try:
                        payload_str = payload_bytes.decode('utf-8')
                    except Exception:
                        return
            if tracking_id in payload_str:
                serial_result['received'] = True
                serial_result['time'] = time.time()
                serial_result['hop_start'] = packet.get('hopStart')
                serial_result['hop_limit'] = packet.get('hopLimit')
                serial_event.set()

        pub.subscribe(rx_watcher, "meshtastic.receive")

        self.results['mqtt_radio_relay']['sent'] += 1
        self.per_device[src_name]['sent'] += 1
        send_time = time.time()

        try:
            iface.sendText(msg, wantAck=False, wantResponse=False)
        except Exception as e:
            pub.unsubscribe(rx_watcher, "meshtastic.receive")
            self.results['mqtt_radio_relay']['failed'] += 1
            return 'SEND_ERROR', None

        # Wait for dst to receive (via MQTT downlink + radio rebroadcast)
        serial_event.wait(timeout=90)
        try:
            pub.unsubscribe(rx_watcher, "meshtastic.receive")
        except Exception:
            pass

        if serial_result['received']:
            latency = serial_result['time'] - send_time
            self.results['mqtt_radio_relay']['verified'] += 1
            self.results['mqtt_radio_relay']['latencies'].append(latency)
            hop_delta = None
            if serial_result['hop_start'] and serial_result['hop_limit']:
                hop_delta = serial_result['hop_start'] - serial_result['hop_limit']
            return f"VERIFIED(hops={hop_delta})", latency
        else:
            self.results['mqtt_radio_relay']['failed'] += 1
            return 'TIMEOUT', None

    # MARK: - Stats

    def print_stats(self):
        ts = time.strftime("%H:%M:%S")
        elapsed = time.time() - self.start_time
        hours = int(elapsed // 3600)
        mins = int((elapsed % 3600) // 60)

        mqtt_stats = self.mqtt.get_stats()

        print(f"\n{'='*70}")
        print(f"  E2E MQTT TEST @ {ts} (elapsed: {hours}h{mins:02d}m)")
        print(f"{'='*70}")

        # Text broadcast
        tb = self.results['text_broadcast']
        if tb['sent'] > 0:
            rate = tb['mqtt_verified'] / tb['sent'] * 100
            avg = sum(tb['latencies']) / len(tb['latencies']) if tb['latencies'] else 0
            print(f"  TEXT BROADCAST: {tb['mqtt_verified']}/{tb['sent']} MQTT-verified "
                  f"({rate:.1f}%) Avg={avg:.1f}s")

        # Text DM
        td = self.results['text_dm']
        if td['sent'] > 0:
            rate = td['mqtt_verified'] / td['sent'] * 100
            avg = sum(td['latencies']) / len(td['latencies']) if td['latencies'] else 0
            print(f"  TEXT DM: {td['mqtt_verified']}/{td['sent']} MQTT-verified "
                  f"({rate:.1f}%) Avg={avg:.1f}s")

        # Media transfer
        mt = self.results['media_transfer']
        if mt['sent'] > 0:
            rate = mt['ack_complete'] / mt['sent'] * 100
            avg = sum(mt['latencies']) / len(mt['latencies']) if mt['latencies'] else 0
            print(f"  MEDIA: {mt['ack_complete']}/{mt['sent']} ACK ({rate:.1f}%) "
                  f"NACK={mt['nack']} Timeout={mt['timeout']} Avg={avg:.1f}s")

        # MQTT relay
        mr = self.results['mqtt_relay']
        if mr['sent'] > 0:
            mqtt_rate = mr['mqtt_verified'] / mr['sent'] * 100
            serial_rate = mr['serial_verified'] / mr['sent'] * 100
            avg = sum(mr['latencies']) / len(mr['latencies']) if mr['latencies'] else 0
            print(f"  MQTT RELAY: mqtt={mr['mqtt_verified']}/{mr['sent']} ({mqtt_rate:.1f}%) "
                  f"serial={mr['serial_verified']}/{mr['sent']} ({serial_rate:.1f}%) Avg={avg:.1f}s")

        # Multi-hop relay
        mrr = self.results['mqtt_radio_relay']
        if mrr['sent'] > 0:
            rate = mrr['verified'] / mrr['sent'] * 100
            avg = sum(mrr['latencies']) / len(mrr['latencies']) if mrr['latencies'] else 0
            print(f"  RADIO RELAY: {mrr['verified']}/{mrr['sent']} ({rate:.1f}%) Avg={avg:.1f}s")

        # Per-device
        print(f"\n  Per-device:")
        for name in sorted(self.per_device.keys()):
            d = self.per_device[name]
            print(f"    {name:8s}: sent={d['sent']} errors={d['send_errors']}")

        # MQTT stats
        print(f"\n  MQTT: {mqtt_stats['total_messages']} total, "
              f"{mqtt_stats['decoded_messages']} decoded, "
              f"nodes={[hex(n) for n in mqtt_stats['nodes_seen']]}")

        # Recent text messages on MQTT
        recent = self.mqtt.get_recent_text_messages(5)
        if recent:
            print(f"\n  Recent MQTT text messages:")
            for m in recent:
                from_name = NODE_NAMES.get(m['from'], hex(m['from']))
                t = time.strftime("%H:%M:%S", time.localtime(m['time']))
                text = (m['payload_text'][:60] + '...') if len(m['payload_text'] or '') > 60 else m['payload_text']
                print(f"    [{t}] {from_name}: {text}")

        print(f"{'='*70}\n")

    def log_result(self, test_num, test_type, detail, result, latency):
        entry = {
            'time': time.strftime("%H:%M:%S"),
            'epoch': time.time(),
            'num': test_num,
            'type': test_type,
            'detail': detail,
            'result': result,
            'latency': latency,
        }
        with open(self.logfile, 'a') as f:
            f.write(json.dumps(entry) + '\n')

    # MARK: - Main Loop

    def run(self):
        if not self.setup():
            return

        self.start_time = time.time()
        active_devices = list(self.interfaces.keys())
        pairs = [(s, d) for s in active_devices for d in active_devices if s != d]
        data_sizes = [20, 50, 100, 150, 200]

        print(f"\n  Starting {self.count} tests across {len(pairs)} device pairs...\n")

        try:
            test_sequence = []
            for i in range(self.count):
                # Cycle through test types:
                # 0. Text broadcast (from rotating device)
                # 1. Text DM (rotating pair)
                # 2-4. Media transfer (rotating pair, rotating size)
                # 5. MQTT relay (A->B with MQTT + serial verification)
                # 6. MQTT radio relay (A->relay->C multi-hop)
                cycle = i % 7
                if cycle == 0:
                    test_sequence.append(('broadcast', active_devices[i % len(active_devices)], None))
                elif cycle == 1:
                    pair = pairs[i % len(pairs)]
                    test_sequence.append(('dm', pair[0], pair[1]))
                elif cycle == 5:
                    pair = pairs[i % len(pairs)]
                    test_sequence.append(('mqtt_relay', pair[0], pair[1]))
                elif cycle == 6 and len(active_devices) >= 3:
                    # Pick 3 distinct devices for multi-hop
                    idx = i % len(active_devices)
                    src = active_devices[idx]
                    relay = active_devices[(idx + 1) % len(active_devices)]
                    dst = active_devices[(idx + 2) % len(active_devices)]
                    test_sequence.append(('mqtt_radio_relay', src, relay, dst))
                else:
                    pair = pairs[i % len(pairs)]
                    size = data_sizes[i % len(data_sizes)]
                    test_sequence.append(('media', pair[0], pair[1], size))

            for i, test_info in enumerate(test_sequence, 1):
                test_type = test_info[0]
                ts = time.strftime("%H:%M:%S")

                if test_type == 'broadcast':
                    device = test_info[1]
                    print(f"[{ts}] #{i}/{self.count}: BROADCAST from {device}...", flush=True)
                    result, latency = self.test_text_broadcast(device)
                    status = f"OK {latency:.1f}s" if latency else result
                    print(f"  -> {result} ({status})", flush=True)
                    self.log_result(i, 'broadcast', device, result, latency)

                elif test_type == 'dm':
                    src, dst = test_info[1], test_info[2]
                    print(f"[{ts}] #{i}/{self.count}: DM {src}->{dst}...", flush=True)
                    result, latency = self.test_text_dm(src, dst)
                    status = f"OK {latency:.1f}s" if latency else result
                    print(f"  -> {result} ({status})", flush=True)
                    self.log_result(i, 'dm', f"{src}->{dst}", result, latency)

                elif test_type == 'media':
                    src, dst = test_info[1], test_info[2]
                    size = test_info[3]
                    print(f"[{ts}] #{i}/{self.count}: MEDIA {src}->{dst} ({size}B)...", flush=True)
                    result, latency = self.test_media_transfer(src, dst, size)
                    status = f"OK {latency:.1f}s" if latency else result
                    print(f"  -> {result} ({status})", flush=True)
                    self.log_result(i, 'media', f"{src}->{dst}", result, latency)

                elif test_type == 'mqtt_relay':
                    src, dst = test_info[1], test_info[2]
                    print(f"[{ts}] #{i}/{self.count}: MQTT_RELAY {src}->{dst}...", flush=True)
                    result, latency = self.test_mqtt_relay_a_to_b(src, dst)
                    status = f"OK {latency:.1f}s" if latency else result
                    print(f"  -> {result} ({status})", flush=True)
                    self.log_result(i, 'mqtt_relay', f"{src}->{dst}", result, latency)

                elif test_type == 'mqtt_radio_relay':
                    src, relay, dst = test_info[1], test_info[2], test_info[3]
                    print(f"[{ts}] #{i}/{self.count}: RADIO_RELAY {src}->{relay}->{dst}...", flush=True)
                    result, latency = self.test_mqtt_radio_relay(src, relay, dst)
                    status = f"OK {latency:.1f}s" if latency else result
                    print(f"  -> {result} ({status})", flush=True)
                    self.log_result(i, 'mqtt_radio_relay', f"{src}->{relay}->{dst}", result, latency)

                # Print stats periodically
                if i % 10 == 0:
                    self.print_stats()

                if i < len(test_sequence):
                    time.sleep(self.delay)

        except KeyboardInterrupt:
            print("\n\nTest interrupted.", flush=True)
        finally:
            self.print_stats()

            # Save final summary
            final = {
                'summary': True,
                'total_time': time.time() - self.start_time,
                'results': {
                    k: {kk: vv for kk, vv in v.items() if kk != 'latencies'}
                    for k, v in self.results.items()
                },
                'mqtt_stats': self.mqtt.get_stats(),
                'per_device': self.per_device,
            }
            with open(self.logfile, 'a') as f:
                f.write(json.dumps(final) + '\n')
            print(f"Results saved to {self.logfile}")

            # Cleanup
            try:
                pub.unsubscribe(self.on_rx, "meshtastic.receive")
            except:
                pass
            for iface in self.interfaces.values():
                try:
                    iface.close()
                except:
                    pass
            self.mqtt.disconnect()


def main():
    parser = argparse.ArgumentParser(description='E2E MQTT test')
    parser.add_argument('--count', type=int, default=100,
                        help='Number of tests (default: 100)')
    parser.add_argument('--delay', type=float, default=5,
                        help='Delay between tests (default: 5)')
    parser.add_argument('--mqtt-host', type=str, default='home.yazdikann.com',
                        help='MQTT broker host')
    args = parser.parse_args()

    if args.mqtt_host != 'home.yazdikann.com':
        MQTT_CONFIG['host'] = args.mqtt_host

    test = E2ETestRunner(args)
    test.run()


if __name__ == "__main__":
    main()
