#!/usr/bin/env python3
"""Diagnostic: media transfer WITHOUT wantAck on individual packets.
Reduces TX queue contention — the media protocol provides its own ACK."""
import sys, time, random, threading
import meshtastic
import meshtastic.serial_interface
from pubsub import pub

VHF_A_PORT = "/dev/cu.usbmodem101"
VHF_B_PORT = "/dev/cu.usbmodem1101"
VHF_A_ID   = 0x335e1be8
VHF_B_ID   = 0x335e1bdc

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

def decode_payload(payload):
    if not isinstance(payload, (bytes, bytearray)) or len(payload) == 0:
        return "?", {}
    pos = 0; found = {}
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
            found[fn] = v
        elif wt == 2:
            ln = 0; shift = 0
            while pos < len(payload):
                b = payload[pos]; pos += 1; ln |= (b & 0x7F) << shift; shift += 7
                if not (b & 0x80): break
            found[f"f{fn}_bytes"] = ln; pos += ln
        else: break
    t = found.get(1, 0)
    return {0:"CHUNK",1:"START",2:"COMPLETE",3:"NACK",4:"ACK_COMPLETE",5:"CANCEL"}.get(t,f"?{t}"), found

def run_test(src_port, dst_id, src_name, dst_name, delay=5, want_ack=False):
    print(f"\n{'='*60}")
    print(f"TEST: {src_name} -> {dst_name}, delay={delay}s, wantAck={want_ack}")
    print(f"{'='*60}")

    data = bytes([(i * 37 + 0x42) & 0xFF for i in range(100)])
    checksum = crc32(data)
    tid = random.randint(0x10000, 0xFFFFFFF)

    start_pkt = bytearray()
    start_pkt.extend(enc_fv(1, 1)); start_pkt.extend(enc_fv(2, tid))
    start_pkt.extend(enc_fv(4, 1)); start_pkt.extend(enc_fv(5, 100))
    start_pkt.extend(enc_fv(9, checksum))

    chunk_pkt = bytearray()
    chunk_pkt.extend(enc_fv(2, tid)); chunk_pkt.extend(enc_fb(6, data))

    complete_pkt = bytearray()
    complete_pkt.extend(enc_fv(1, 2)); complete_pkt.extend(enc_fv(2, tid))
    complete_pkt.extend(enc_fv(9, checksum))

    print(f"  tid=0x{tid:08X}, checksum=0x{checksum:08X}")

    ack_event = threading.Event()
    ack_result = [None]
    all_events = []

    def on_rx(packet, interface):
        decoded = packet.get("decoded", {})
        portnum = decoded.get("portnum", "?")
        fromNode = packet.get("fromId", "?")
        toNode = packet.get("toId", "?")
        payload = decoded.get("payload", b"")
        ts = time.strftime("%H:%M:%S")

        extra = ""
        if portnum == "ROUTING_APP":
            routing = decoded.get("routing", {})
            extra = f" err={routing.get('errorReason', '?')}"
        elif portnum in (259, "MEDIA_TRANSFER_APP"):
            mtype, fields = decode_payload(payload)
            extra = f" type={mtype}"
            if mtype in ("ACK_COMPLETE", "NACK"):
                ack_result[0] = {"type": mtype, "from": fromNode}
                ack_event.set()

        msg = f"  [{ts}] RX: {fromNode}->{toNode} {portnum}{extra}"
        print(msg, flush=True)
        all_events.append(msg)

    iface = meshtastic.serial_interface.SerialInterface(src_port)
    time.sleep(2)
    pub.subscribe(on_rx, "meshtastic.receive")

    ts = time.strftime("%H:%M:%S")
    print(f"\n  [{ts}] TX: MEDIA_START", flush=True)
    iface.sendData(bytes(start_pkt), destinationId=dst_id, portNum=259,
                   wantAck=want_ack, wantResponse=False)
    time.sleep(delay)

    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] TX: MEDIA_CHUNK", flush=True)
    iface.sendData(bytes(chunk_pkt), destinationId=dst_id, portNum=259,
                   wantAck=want_ack, wantResponse=False)
    time.sleep(delay)

    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] TX: MEDIA_COMPLETE", flush=True)
    iface.sendData(bytes(complete_pkt), destinationId=dst_id, portNum=259,
                   wantAck=want_ack, wantResponse=False)

    print(f"\n  Waiting 120s for ACK_COMPLETE or NACK...", flush=True)
    ack_event.wait(timeout=120)

    if ack_result[0]:
        r = ack_result[0]
        print(f"\n  *** {r['type']} from {r['from']} ***", flush=True)
    else:
        print(f"\n  *** NO media response in 120s ***", flush=True)

    time.sleep(3)
    try: pub.unsubscribe(on_rx, "meshtastic.receive")
    except: pass
    iface.close()
    time.sleep(2)
    return ack_result[0]

def main():
    results = {}

    # Test 1: VHF-B -> VHF-A, NO wantAck, 5s delay
    r = run_test(VHF_B_PORT, VHF_A_ID, "VHF-B", "VHF-A", delay=5, want_ack=False)
    results["B->A noAck"] = r["type"] if r else "NONE"

    # Test 2: VHF-A -> VHF-B, NO wantAck, 5s delay
    r = run_test(VHF_A_PORT, VHF_B_ID, "VHF-A", "VHF-B", delay=5, want_ack=False)
    results["A->B noAck"] = r["type"] if r else "NONE"

    print(f"\n{'='*60}")
    print("RESULTS:")
    for k, v in results.items():
        status = "OK" if v == "ACK_COMPLETE" else ("NACK" if v == "NACK" else "FAIL")
        print(f"  {k}: {v} [{status}]")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
