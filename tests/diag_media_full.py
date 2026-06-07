#!/usr/bin/env python3
"""Diagnostic: full media transfer with generous delays for TX queue clearance."""
import sys, time, random, threading, os, subprocess, re
import meshtastic
import meshtastic.serial_interface
from pubsub import pub

MESHTASTIC = "/Users/patrick/Library/Python/3.14/bin/meshtastic"

PORTS = {
    "VHF-A": ("/dev/cu.usbmodem101",  0x335e1be8),
    "VHF-B": ("/dev/cu.usbmodem1101", 0x335e1bdc),
    "BPF-A": ("/dev/cu.usbmodem21101", None),
    "BPF-B": ("/dev/cu.usbmodem21201", None),
}

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

def read_varint(data, pos):
    value = shift = 0
    while pos < len(data):
        b = data[pos]; pos += 1
        value |= (b & 0x7F) << shift; shift += 7
        if not (b & 0x80): break
    return value, pos

def decode_media_type(payload):
    if not isinstance(payload, (bytes, bytearray)) or len(payload) == 0:
        return "?"
    pos = 0
    found_type = None
    while pos < len(payload):
        tw, pos = read_varint(payload, pos)
        fn = tw >> 3; wt = tw & 7
        if wt == 0:
            v, pos = read_varint(payload, pos)
            if fn == 1:
                found_type = v
        elif wt == 2:
            ln, pos = read_varint(payload, pos)
            pos += ln
        else:
            break
    if found_type is not None:
        return {0:"CHUNK", 1:"START", 2:"COMPLETE", 3:"NACK", 4:"ACK_COMPLETE", 5:"CANCEL"}.get(found_type, f"?{found_type}")
    return "CHUNK(default)"  # type=0 omitted = MEDIA_CHUNK

def test_transfer(src_name, src_port, dst_name, dst_id, delay=15):
    print(f"\n{'='*60}", flush=True)
    print(f"Transfer: {src_name} -> {dst_name} (0x{dst_id:08x}), delay={delay}s", flush=True)

    data = bytes([(i * 37 + 0x42) & 0xFF for i in range(100)])
    checksum = crc32(data)
    tid = random.randint(0x10000, 0xFFFFFFF)

    start_pkt = bytearray()
    start_pkt.extend(enc_fv(1, 1))      # MEDIA_START
    start_pkt.extend(enc_fv(2, tid))
    start_pkt.extend(enc_fv(4, 1))      # total_chunks=1
    start_pkt.extend(enc_fv(5, 100))    # total_size=100
    start_pkt.extend(enc_fv(9, checksum))

    chunk_pkt = bytearray()
    chunk_pkt.extend(enc_fv(2, tid))
    chunk_pkt.extend(enc_fb(6, data))

    complete_pkt = bytearray()
    complete_pkt.extend(enc_fv(1, 2))   # MEDIA_COMPLETE
    complete_pkt.extend(enc_fv(2, tid))
    complete_pkt.extend(enc_fv(9, checksum))

    print(f"  tid=0x{tid:08X}, checksum=0x{checksum:08X}", flush=True)

    ack_event = threading.Event()
    ack_result = [None]

    def on_rx(packet, interface):
        decoded = packet.get("decoded", {})
        portnum = decoded.get("portnum", "?")
        fromNode = packet.get("fromId", "?")
        payload = decoded.get("payload", b"")
        ts = time.strftime("%H:%M:%S")

        extra = ""
        if portnum == "ROUTING_APP":
            routing = decoded.get("routing", {})
            extra = f" err={routing.get('errorReason', '?')}"
        elif portnum in (259, "MEDIA_TRANSFER_APP"):
            mtype = decode_media_type(payload) if isinstance(payload, (bytes, bytearray)) else "?"
            extra = f" type={mtype}"
            ack_result[0] = {"type": mtype, "packet": packet}
            ack_event.set()

        print(f"  [{ts}] RX: from={fromNode} portnum={portnum}{extra}", flush=True)

    iface = meshtastic.serial_interface.SerialInterface(src_port)
    time.sleep(2)
    pub.subscribe(on_rx, "meshtastic.receive")

    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] TX: MEDIA_START", flush=True)
    iface.sendData(bytes(start_pkt), destinationId=dst_id, portNum=259,
                   wantAck=True, wantResponse=False)
    time.sleep(delay)

    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] TX: MEDIA_CHUNK (100B)", flush=True)
    iface.sendData(bytes(chunk_pkt), destinationId=dst_id, portNum=259,
                   wantAck=True, wantResponse=False)
    time.sleep(delay)

    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] TX: MEDIA_COMPLETE", flush=True)
    iface.sendData(bytes(complete_pkt), destinationId=dst_id, portNum=259,
                   wantAck=True, wantResponse=False)

    print(f"  Waiting 90s for ACK_COMPLETE...", flush=True)
    ack_event.wait(timeout=90)

    result = "NONE"
    if ack_result[0]:
        result = ack_result[0]["type"]
        if result == "ACK_COMPLETE":
            print(f"  *** ACK_COMPLETE received ***", flush=True)
        elif result == "NACK":
            print(f"  *** NACK received ***", flush=True)
        else:
            print(f"  *** Media response: {result} ***", flush=True)
    else:
        print(f"  NO response in 90s", flush=True)

    try: pub.unsubscribe(on_rx, "meshtastic.receive")
    except: pass
    iface.close()
    time.sleep(2)
    return result

# Auto-detect node IDs
for name in ("BPF-A", "BPF-B"):
    port, _ = PORTS[name]
    if not os.path.exists(port):
        print(f"{name}: port {port} not present", flush=True)
        continue
    try:
        r = subprocess.run([MESHTASTIC, "--port", port, "--info"],
                          capture_output=True, text=True, timeout=15)
        m = re.search(r'"myNodeNum":\s*(\d+)', r.stdout)
        if m:
            nid = int(m.group(1))
            PORTS[name] = (port, nid)
            print(f"{name}: node 0x{nid:08x}", flush=True)
    except: pass

# Test all available pairs
results = {}
pairs = [
    ("VHF-A", "VHF-B"),
    ("VHF-B", "VHF-A"),
    ("VHF-A", "BPF-A"),
    ("VHF-B", "BPF-A"),
    ("BPF-A", "VHF-B"),
    ("VHF-B", "BPF-B"),
]

for src, dst in pairs:
    src_port, _ = PORTS[src]
    _, dst_id = PORTS[dst]
    if not os.path.exists(src_port) or dst_id is None:
        print(f"Skipping {src}->{dst}: unavailable", flush=True)
        continue
    r = test_transfer(src, src_port, dst, dst_id, delay=15)
    results[f"{src}->{dst}"] = r

print(f"\n{'='*60}", flush=True)
print(f"SUMMARY:", flush=True)
for pair, result in results.items():
    status = "OK" if result == "ACK_COMPLETE" else ("NACK" if result == "NACK" else "FAIL")
    print(f"  {pair:<20} {result:<15} [{status}]", flush=True)
