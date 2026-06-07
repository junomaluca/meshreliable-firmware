#!/usr/bin/env python3
"""Diagnostic: capture VHF-B firmware logs while sending media packets from VHF-A.
Uses meshtastic library's log capture to see firmware LOG_INFO/LOG_DEBUG messages."""
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

    # Subscribe to ALL log events BEFORE opening interfaces
    log_lines = []
    def on_log(line, interface=None):
        ts = time.strftime("%H:%M:%S")
        msg = f"  [{ts}] LOG: {line.rstrip()}"
        print(msg, flush=True)
        log_lines.append(msg)

    def on_rx(packet, interface):
        decoded = packet.get("decoded", {})
        portnum = decoded.get("portnum", "?")
        fromNode = packet.get("fromId", "?")
        toNode = packet.get("toId", "?")
        payload = decoded.get("payload", b"")
        ts = time.strftime("%H:%M:%S")
        # Identify which interface
        iface_label = "?"
        try:
            if hasattr(interface, 'devPath'):
                if '101' in str(interface.devPath) and '1101' not in str(interface.devPath):
                    iface_label = "VHF-A"
                elif '1101' in str(interface.devPath):
                    iface_label = "VHF-B"
        except: pass

        if portnum == "ROUTING_APP":
            routing = decoded.get("routing", {})
            err = routing.get('errorReason', '?')
            if err != "NONE":
                print(f"  [{ts}] {iface_label}: {fromNode}->{toNode} ROUTING err={err}", flush=True)
        elif portnum in (259, "MEDIA_TRANSFER_APP"):
            mtype = decode_type(payload)
            print(f"  [{ts}] {iface_label}: {fromNode}->{toNode} MEDIA type={mtype}", flush=True)

    pub.subscribe(on_log, "meshtastic.log.line")
    pub.subscribe(on_rx, "meshtastic.receive")

    print(f"{'='*60}")
    print(f"LOG CAPTURE DIAGNOSTIC")
    print(f"Capturing VHF-B firmware logs while sending from VHF-A")
    print(f"{'='*60}\n")

    # Open VHF-B FIRST (to capture logs)
    print("Opening VHF-B (log capture)...", flush=True)
    iface_b = meshtastic.serial_interface.SerialInterface(VHF_B_PORT)
    time.sleep(3)

    # Open VHF-A (sender)
    print("Opening VHF-A (sender)...", flush=True)
    iface_a = meshtastic.serial_interface.SerialInterface(VHF_A_PORT)
    time.sleep(3)

    # Wait for boot noise to settle
    print("Waiting 5s for settle...\n", flush=True)
    time.sleep(5)

    # Build packets
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

    print(f"tid=0x{tid:08X}, checksum=0x{checksum:08X}")
    print(f"start_pkt ({len(start_pkt)} bytes): {start_pkt.hex()}")
    print(f"chunk_pkt ({len(chunk_pkt)} bytes): {chunk_pkt[:20].hex()}...")
    print(f"complete_pkt ({len(complete_pkt)} bytes): {complete_pkt.hex()}")
    print()

    # Send START
    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] >>> TX: START", flush=True)
    iface_a.sendData(bytes(start_pkt), destinationId=VHF_B_ID, portNum=259,
                     wantAck=False, wantResponse=False)
    time.sleep(delay)

    # Send CHUNK
    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] >>> TX: CHUNK", flush=True)
    iface_a.sendData(bytes(chunk_pkt), destinationId=VHF_B_ID, portNum=259,
                     wantAck=False, wantResponse=False)
    time.sleep(delay)

    # Send COMPLETE
    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] >>> TX: COMPLETE", flush=True)
    iface_a.sendData(bytes(complete_pkt), destinationId=VHF_B_ID, portNum=259,
                     wantAck=False, wantResponse=False)

    # Wait for ACK_COMPLETE
    print(f"  Waiting 60s for ACK_COMPLETE or log messages...", flush=True)
    time.sleep(60)

    # Summary
    print(f"\n{'='*60}")
    print(f"LOG SUMMARY: {len(log_lines)} log messages captured")
    media_logs = [l for l in log_lines if 'MediaXfer' in l or 'media' in l.lower()]
    print(f"Media-related: {len(media_logs)}")
    for l in media_logs:
        print(l)
    print(f"{'='*60}")

    # Cleanup
    try: pub.unsubscribe(on_log, "meshtastic.log.line")
    except: pass
    try: pub.unsubscribe(on_rx, "meshtastic.receive")
    except: pass
    iface_a.close()
    iface_b.close()

if __name__ == "__main__":
    main()
