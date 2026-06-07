#!/usr/bin/env python3
"""MQTT observer: monitors MQTT traffic in real-time while stress test runs.
Provides insight into which messages appear on MQTT, which devices relay, etc.

This runs alongside the stress test (doesn't touch serial ports).
Usage: python3 mqtt_observer.py
"""
import time, json, sys, struct, threading
import paho.mqtt.client as mqtt
from meshtastic.protobuf import mqtt_pb2, portnums_pb2, mesh_pb2
from meshtastic_crypto import decrypt_mesh_packet

MQTT_CONFIG = {
    'host': 'home.yazdikann.com',
    'port': 1883,
    'username': 'admin',
    'password': 'admin',
    'topic_root': 'msh/US',
}

# Known device IDs
KNOWN_DEVICES = {
    0x335e1be8: 'VHF-A',
    0x335e1bdc: 'VHF-B',
    0x5e23dd3d: 'BPF-A',
    0x3ce5c32b: 'BPF-B',
}

class MQTTObserver:
    def __init__(self):
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"observer_{int(time.time())}")
        self.client.username_pw_set(MQTT_CONFIG['username'], MQTT_CONFIG['password'])
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.stats = {
            'total': 0,
            'decoded': 0,
            'encrypted': 0,
            'by_portnum': {},
            'by_sender': {},
            'by_topic': {},
            'text_messages': [],
            'media_events': [],
        }
        self.start_time = None
        self.lock = threading.Lock()

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            topic = f"{MQTT_CONFIG['topic_root']}/#"
            client.subscribe(topic)
            ts = time.strftime("%H:%M:%S")
            print(f"[{ts}] Connected to MQTT, subscribed to {topic}")
        else:
            print(f"MQTT connection failed: {reason_code}")

    def _on_message(self, client, userdata, msg, *args):
        with self.lock:
            self.stats['total'] += 1

        # Track topic
        topic_parts = msg.topic.split('/')
        if len(topic_parts) >= 5:
            channel = topic_parts[4] if len(topic_parts) > 4 else '?'
            with self.lock:
                self.stats['by_topic'][channel] = self.stats['by_topic'].get(channel, 0) + 1

        try:
            env = mqtt_pb2.ServiceEnvelope()
            env.ParseFromString(msg.payload)

            if env.HasField('packet'):
                pkt = env.packet
                from_id = pkt.from_ if hasattr(pkt, 'from_') else getattr(pkt, 'from', 0)
                to_id = pkt.to
                sender_name = KNOWN_DEVICES.get(from_id, f"!{from_id:08x}")

                with self.lock:
                    self.stats['by_sender'][sender_name] = self.stats['by_sender'].get(sender_name, 0) + 1

                if pkt.HasField('decoded'):
                    portnum = pkt.decoded.portnum
                    portnum_name = portnums_pb2.PortNum.Name(portnum) if portnum else "UNKNOWN"
                    payload = pkt.decoded.payload

                    with self.lock:
                        self.stats['decoded'] += 1
                        self.stats['by_portnum'][portnum_name] = self.stats['by_portnum'].get(portnum_name, 0) + 1

                    # Log interesting events
                    ts = time.strftime("%H:%M:%S")

                    if portnum == portnums_pb2.TEXT_MESSAGE_APP:
                        try:
                            text = payload.decode('utf-8')
                            to_name = KNOWN_DEVICES.get(to_id, f"!{to_id:08x}")
                            ch = getattr(pkt, 'channel', 0)
                            print(f"[{ts}] TEXT {sender_name} -> {to_name} ch={ch}: {text[:80]}")
                            with self.lock:
                                self.stats['text_messages'].append({
                                    'time': ts, 'from': sender_name, 'to': to_name,
                                    'channel': ch, 'text': text[:200],
                                })
                        except:
                            pass

                    elif portnum == 259:  # MEDIA_TRANSFER_APP
                        print(f"[{ts}] MEDIA {sender_name} -> !{to_id:08x} ({len(payload)}B)")
                        with self.lock:
                            self.stats['media_events'].append({
                                'time': ts, 'from': sender_name,
                                'to': KNOWN_DEVICES.get(to_id, f"!{to_id:08x}"),
                                'size': len(payload),
                            })

                    elif portnum == portnums_pb2.POSITION_APP:
                        pass  # silently count positions

                    elif portnum == portnums_pb2.TELEMETRY_APP:
                        pass  # silently count telemetry

                    elif portnum == portnums_pb2.NODEINFO_APP:
                        pass  # silently count node info

                    else:
                        if portnum not in (portnums_pb2.ROUTING_APP, portnums_pb2.MAP_REPORT_APP):
                            print(f"[{ts}] {portnum_name} from {sender_name} ({len(payload)}B)")

                elif pkt.HasField('encrypted'):
                    # Try to decrypt using channel PSKs
                    channel_id = None
                    if len(topic_parts) > 4:
                        channel_id = topic_parts[4]
                    data = decrypt_mesh_packet(pkt, channel_id)
                    if data:
                        portnum = data.portnum
                        portnum_name = portnums_pb2.PortNum.Name(portnum) if portnum else "UNKNOWN"
                        payload = data.payload

                        with self.lock:
                            self.stats['decoded'] += 1
                            self.stats['by_portnum'][portnum_name] = self.stats['by_portnum'].get(portnum_name, 0) + 1

                        ts = time.strftime("%H:%M:%S")

                        if portnum == portnums_pb2.TEXT_MESSAGE_APP:
                            try:
                                text = payload.decode('utf-8')
                                to_name = KNOWN_DEVICES.get(to_id, f"!{to_id:08x}")
                                print(f"[{ts}] TEXT(dec) {sender_name} -> {to_name}: {text[:80]}")
                                with self.lock:
                                    self.stats['text_messages'].append({
                                        'time': ts, 'from': sender_name, 'to': to_name,
                                        'channel': channel_id or '?', 'text': text[:200],
                                    })
                            except Exception:
                                pass
                        elif portnum == 259:  # MEDIA_TRANSFER_APP
                            print(f"[{ts}] MEDIA(dec) {sender_name} -> !{to_id:08x} ({len(payload)}B)")
                            with self.lock:
                                self.stats['media_events'].append({
                                    'time': ts, 'from': sender_name,
                                    'to': KNOWN_DEVICES.get(to_id, f"!{to_id:08x}"),
                                    'size': len(payload),
                                })
                        elif portnum not in (portnums_pb2.POSITION_APP, portnums_pb2.TELEMETRY_APP,
                                             portnums_pb2.NODEINFO_APP, portnums_pb2.ROUTING_APP,
                                             portnums_pb2.MAP_REPORT_APP):
                            print(f"[{ts}] {portnum_name}(dec) from {sender_name} ({len(payload)}B)")
                    else:
                        with self.lock:
                            self.stats['encrypted'] += 1
        except Exception as e:
            pass

    def print_stats(self):
        with self.lock:
            stats = dict(self.stats)
            elapsed = time.time() - self.start_time
            hours = int(elapsed // 3600)
            mins = int((elapsed % 3600) // 60)

        ts = time.strftime("%H:%M:%S")
        print(f"\n{'='*60}")
        print(f"  MQTT OBSERVER @ {ts} (elapsed: {hours}h{mins:02d}m)")
        print(f"{'='*60}")
        print(f"  Total packets: {stats['total']}")
        print(f"  Decoded: {stats['decoded']}, Encrypted: {stats['encrypted']}")

        if stats['by_portnum']:
            print(f"\n  By portnum:")
            for pn, count in sorted(stats['by_portnum'].items(), key=lambda x: -x[1]):
                print(f"    {pn:30s}: {count}")

        if stats['by_sender']:
            print(f"\n  By sender:")
            for sender, count in sorted(stats['by_sender'].items(), key=lambda x: -x[1]):
                print(f"    {sender:15s}: {count}")

        if stats['by_topic']:
            print(f"\n  By channel/topic:")
            for topic, count in sorted(stats['by_topic'].items(), key=lambda x: -x[1]):
                print(f"    {topic:15s}: {count}")

        recent_texts = stats['text_messages'][-5:]
        if recent_texts:
            print(f"\n  Recent text messages:")
            for m in recent_texts:
                print(f"    [{m['time']}] {m['from']} -> {m['to']} ch{m['channel']}: {m['text'][:60]}")

        recent_media = stats['media_events'][-5:]
        if recent_media:
            print(f"\n  Recent media events:")
            for m in recent_media:
                print(f"    [{m['time']}] {m['from']} -> {m['to']} ({m['size']}B)")

        print(f"{'='*60}\n")

    def run(self):
        self.start_time = time.time()
        self.client.connect(MQTT_CONFIG['host'], MQTT_CONFIG['port'], 60)
        self.client.loop_start()

        # Wait for connection
        time.sleep(3)

        try:
            while True:
                time.sleep(60)
                self.print_stats()
        except KeyboardInterrupt:
            print("\nObserver stopped.")
            self.print_stats()
        finally:
            self.client.loop_stop()
            self.client.disconnect()


def main():
    print("Starting MQTT observer...")
    print(f"  Host: {MQTT_CONFIG['host']}:{MQTT_CONFIG['port']}")
    print(f"  Known devices: {', '.join(f'{n} ({hex(i)})' for i, n in KNOWN_DEVICES.items())}")
    print()

    observer = MQTTObserver()
    observer.run()


if __name__ == "__main__":
    main()
