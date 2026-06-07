#!/usr/bin/env python3
"""Diagnostic: bidirectional media transfer with dual monitoring.
Tests both directions, listening on both devices."""
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

def decode_type(payload):
    if not isinstance(payload, (bytes, bytearray)) or len(payload) == 0: return "?"
    pos = 0; found_type = None
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
            if fn == 1: found_type = v
        elif wt == 2:
            ln = 0; shift = 0
            while pos < len(payload):
                b = payload[pos]; pos += 1; ln |= (b & 0x7F) << shift; shift += 7
                if not (b & 0x80): break
            pos += ln
        else: break
    if found_type is not None:
        return {0:"CHUNK",1:"START",2:"COMPLETE",3:"NACK",4:"ACK_COMPLETE",5:"CANCEL"}.get(found_type, f"?{found_type}")
    return "CHUNK"

def main():
    delay = 10
    print(f"{'='*70}")
    print(f"BIDIRECTIONAL: dual-monitor, wantAck=False, delay={delay}s")
    print(f"ACK_COMPLETE: want_ack=true, RELIABLE priority, default hop_limit")
    print(f"NextHopRouter: !isFromUs only (no local retransmission)")
    print(f"{'='*70}\n")

    # Open both interfaces
    print("Opening VHF-A...", flush=True)
    iface_a = meshtastic.serial_interface.SerialInterface(VHF_A_PORT)
    time.sleep(2)
    print("Opening VHF-B...", flush=True)
    iface_b = meshtastic.serial_interface.SerialInterface(VHF_B_PORT)
    time.sleep(2)

    ack_event = threading.Event()
    ack_result = [None]

    def make_listener(label):
        def on_rx(packet, interface):
            decoded = packet.get("decoded", {})
            portnum = decoded.get("portnum", "?")
            fromNode = packet.get("fromId", "?")
            toNode = packet.get("toId", "?")
            payload = decoded.get("payload", b"")
            ts = time.strftime("%H:%M:%S")

            if portnum == "ROUTING_APP":
                routing = decoded.get("routing", {})
                err = routing.get('errorReason', '?')
                if err != "NONE":
                    print(f"  [{ts}] {label}: {fromNode}->{toNode} ROUTING err={err}", flush=True)
            elif portnum in (259, "MEDIA_TRANSFER_APP"):
                mtype = decode_type(payload)
                print(f"  [{ts}] {label}: {fromNode}->{toNode} MEDIA type={mtype}", flush=True)
                if mtype in ("ACK_COMPLETE", "NACK"):
                    ack_result[0] = {"type": mtype, "from": fromNode, "label": label}
                    ack_event.set()
        return on_rx

    listener_a = make_listener("VHF-A")
    listener_b = make_listener("VHF-B")
    pub.subscribe(listener_a, "meshtastic.receive")
    pub.subscribe(listener_b, "meshtastic.receive")

    # Wait for noise to settle
    time.sleep(5)

    # ---- TEST 1: VHF-A -> VHF-B ----
    print(f"\n{'='*50}")
    print(f"TEST 1: VHF-A -> VHF-B (wantAck=False)")
    print(f"{'='*50}")

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

    print(f"  tid=0x{tid:08X}")

    ack_event.clear()
    ack_result[0] = None

    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] TX: START", flush=True)
    iface_a.sendData(bytes(start_pkt), destinationId=VHF_B_ID, portNum=259,
                     wantAck=False, wantResponse=False)
    time.sleep(delay)

    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] TX: CHUNK", flush=True)
    iface_a.sendData(bytes(chunk_pkt), destinationId=VHF_B_ID, portNum=259,
                     wantAck=False, wantResponse=False)
    time.sleep(delay)

    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] TX: COMPLETE", flush=True)
    iface_a.sendData(bytes(complete_pkt), destinationId=VHF_B_ID, portNum=259,
                     wantAck=False, wantResponse=False)

    print(f"  Waiting 120s...", flush=True)
    ack_event.wait(timeout=120)

    if ack_result[0]:
        r = ack_result[0]
        print(f"\n  *** TEST 1 RESULT: {r['type']} from {r['from']} (seen by {r['label']}) ***", flush=True)
    else:
        print(f"\n  *** TEST 1: NO response in 120s ***", flush=True)

    # Generous gap to let routing ACKs from test 1's want_ack=true ACK_COMPLETE clear
    print(f"  Waiting 30s between tests for TX queue to clear...", flush=True)
    time.sleep(30)

    # ---- TEST 2: VHF-B -> VHF-A ----
    print(f"\n{'='*50}")
    print(f"TEST 2: VHF-B -> VHF-A (wantAck=False)")
    print(f"{'='*50}")

    tid2 = random.randint(0x10000, 0xFFFFFFF)
    start_pkt2 = bytearray()
    start_pkt2.extend(enc_fv(1, 1)); start_pkt2.extend(enc_fv(2, tid2))
    start_pkt2.extend(enc_fv(4, 1)); start_pkt2.extend(enc_fv(5, 100))
    start_pkt2.extend(enc_fv(9, checksum))
    chunk_pkt2 = bytearray()
    chunk_pkt2.extend(enc_fv(2, tid2)); chunk_pkt2.extend(enc_fb(6, data))
    complete_pkt2 = bytearray()
    complete_pkt2.extend(enc_fv(1, 2)); complete_pkt2.extend(enc_fv(2, tid2))
    complete_pkt2.extend(enc_fv(9, checksum))

    print(f"  tid=0x{tid2:08X}")

    ack_event.clear()
    ack_result[0] = None

    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] TX: START", flush=True)
    iface_b.sendData(bytes(start_pkt2), destinationId=VHF_A_ID, portNum=259,
                     wantAck=False, wantResponse=False)
    time.sleep(delay)

    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] TX: CHUNK", flush=True)
    iface_b.sendData(bytes(chunk_pkt2), destinationId=VHF_A_ID, portNum=259,
                     wantAck=False, wantResponse=False)
    time.sleep(delay)

    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] TX: COMPLETE", flush=True)
    iface_b.sendData(bytes(complete_pkt2), destinationId=VHF_A_ID, portNum=259,
                     wantAck=False, wantResponse=False)

    print(f"  Waiting 120s...", flush=True)
    ack_event.wait(timeout=120)

    if ack_result[0]:
        r = ack_result[0]
        print(f"\n  *** TEST 2 RESULT: {r['type']} from {r['from']} (seen by {r['label']}) ***", flush=True)
    else:
        print(f"\n  *** TEST 2: NO response in 120s ***", flush=True)

    # Cleanup
    try: pub.unsubscribe(listener_a, "meshtastic.receive")
    except: pass
    try: pub.unsubscribe(listener_b, "meshtastic.receive")
    except: pass
    iface_a.close()
    iface_b.close()

if __name__ == "__main__":
    main()
