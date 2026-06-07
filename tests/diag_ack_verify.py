#!/usr/bin/env python3
"""Diagnostic: verify ACK_COMPLETE works when all chunks are received.
Uses long delays (30s) between packets to ensure TX queue is clear.
Sends CHUNK twice for redundancy."""
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
    delay = 30  # 30s between packets to let TX queue clear
    ack_event = threading.Event()
    ack_result = [None]

    def on_log(line, interface=None):
        ts = time.strftime("%H:%M:%S")
        clean = line.rstrip()
        # Only show media-related logs
        if 'MediaXfer' in clean or 'media' in clean.lower() or 'Portnum=259' in clean:
            print(f"  [{ts}] LOG: {clean}", flush=True)

    def on_rx(packet, interface):
        decoded = packet.get("decoded", {})
        portnum = decoded.get("portnum", "?")
        fromNode = packet.get("fromId", "?")
        toNode = packet.get("toId", "?")
        payload = decoded.get("payload", b"")
        ts = time.strftime("%H:%M:%S")

        if portnum in (259, "MEDIA_TRANSFER_APP"):
            mtype = decode_type(payload)
            iface_label = "?"
            try:
                if hasattr(interface, 'devPath'):
                    if '101' in str(interface.devPath) and '1101' not in str(interface.devPath):
                        iface_label = "VHF-A"
                    elif '1101' in str(interface.devPath):
                        iface_label = "VHF-B"
            except: pass
            print(f"  [{ts}] {iface_label}: {fromNode}->{toNode} MEDIA type={mtype}", flush=True)
            if mtype in ("ACK_COMPLETE", "NACK"):
                ack_result[0] = {"type": mtype, "from": fromNode}
                ack_event.set()

    pub.subscribe(on_log, "meshtastic.log.line")
    pub.subscribe(on_rx, "meshtastic.receive")

    print(f"{'='*60}")
    print(f"ACK_COMPLETE VERIFICATION TEST")
    print(f"30s delays between packets, CHUNK sent twice")
    print(f"{'='*60}\n")

    print("Opening VHF-B (receiver)...", flush=True)
    iface_b = meshtastic.serial_interface.SerialInterface(VHF_B_PORT)
    time.sleep(3)
    print("Opening VHF-A (sender)...", flush=True)
    iface_a = meshtastic.serial_interface.SerialInterface(VHF_A_PORT)
    time.sleep(3)

    print("Waiting 15s for boot settle and TX queue to drain...\n", flush=True)
    time.sleep(15)

    # Use very small data (20 bytes) for reliable delivery
    data = bytes([(i * 37 + 0x42) & 0xFF for i in range(20)])
    checksum = crc32(data)
    tid = random.randint(0x10000, 0xFFFFFFF)

    start_pkt = bytearray()
    start_pkt.extend(enc_fv(1, 1)); start_pkt.extend(enc_fv(2, tid))
    start_pkt.extend(enc_fv(4, 1)); start_pkt.extend(enc_fv(5, 20))
    start_pkt.extend(enc_fv(7, 3))  # content_type=3 (BINARY_DATA) — avoids VoiceMemo crash
    start_pkt.extend(enc_fv(9, checksum))
    chunk_pkt = bytearray()
    chunk_pkt.extend(enc_fv(2, tid)); chunk_pkt.extend(enc_fb(6, data))
    complete_pkt = bytearray()
    complete_pkt.extend(enc_fv(1, 2)); complete_pkt.extend(enc_fv(2, tid))
    complete_pkt.extend(enc_fv(9, checksum))

    print(f"  tid=0x{tid:08X}, data=20 bytes, checksum=0x{checksum:08X}")

    # Send START
    ts = time.strftime("%H:%M:%S")
    print(f"\n  [{ts}] >>> TX: START (waiting {delay}s)", flush=True)
    iface_a.sendData(bytes(start_pkt), destinationId=VHF_B_ID, portNum=259,
                     wantAck=False, wantResponse=False)
    time.sleep(delay)

    # Send CHUNK (first attempt)
    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] >>> TX: CHUNK #1 (waiting {delay}s)", flush=True)
    iface_a.sendData(bytes(chunk_pkt), destinationId=VHF_B_ID, portNum=259,
                     wantAck=False, wantResponse=False)
    time.sleep(delay)

    # Send CHUNK again (redundancy)
    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] >>> TX: CHUNK #2 redundant (waiting {delay}s)", flush=True)
    iface_a.sendData(bytes(chunk_pkt), destinationId=VHF_B_ID, portNum=259,
                     wantAck=False, wantResponse=False)
    time.sleep(delay)

    # Send COMPLETE
    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] >>> TX: COMPLETE", flush=True)
    iface_a.sendData(bytes(complete_pkt), destinationId=VHF_B_ID, portNum=259,
                     wantAck=False, wantResponse=False)

    print(f"  Waiting 120s for ACK_COMPLETE...", flush=True)
    ack_event.wait(timeout=120)

    if ack_result[0]:
        r = ack_result[0]
        print(f"\n  *** RESULT: {r['type']} from {r['from']} ***", flush=True)
        if r['type'] == 'ACK_COMPLETE':
            print(f"  *** SUCCESS: ACK_COMPLETE received! ***", flush=True)
        elif r['type'] == 'NACK':
            print(f"  *** PARTIAL: NACK received (chunk still lost) ***", flush=True)
    else:
        print(f"\n  *** FAIL: NO response in 120s ***", flush=True)

    # Cleanup
    try: pub.unsubscribe(on_log, "meshtastic.log.line")
    except: pass
    try: pub.unsubscribe(on_rx, "meshtastic.receive")
    except: pass
    iface_a.close()
    iface_b.close()

if __name__ == "__main__":
    main()
