#!/usr/bin/env python3
"""Diagnostic: single media transfer with serial console monitoring on both devices.
Shows firmware-side MediaXfer logs alongside Python-side events."""
import sys, time, random, threading, serial, os
import meshtastic
import meshtastic.serial_interface
from pubsub import pub

# Device config
VHF_A_PORT = "/dev/cu.usbmodem101"
VHF_B_PORT = "/dev/cu.usbmodem1101"
VHF_A_ID   = 0x335e1be8
VHF_B_ID   = 0x335e1bdc

# Which direction to test
SRC_PORT = VHF_B_PORT   # sender
DST_ID   = VHF_A_ID     # receiver
DST_PORT = VHF_A_PORT   # receiver serial for monitoring
SRC_NAME = "VHF-B"
DST_NAME = "VHF-A"

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

def serial_monitor(port, label, stop_event):
    """Read serial lines from device, print with label prefix."""
    try:
        ser = serial.Serial(port, 115200, timeout=0.5)
        while not stop_event.is_set():
            try:
                line = ser.readline()
                if line:
                    text = line.decode('utf-8', errors='replace').strip()
                    if text and ('MediaXfer' in text or 'media' in text.lower() or 'NACK' in text or 'ACK' in text):
                        ts = time.strftime("%H:%M:%S")
                        print(f"  [{ts}] SERIAL-{label}: {text}", flush=True)
            except Exception:
                pass
        ser.close()
    except Exception as e:
        print(f"  Serial monitor {label} failed: {e}", flush=True)

def main():
    delay = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    print(f"{'='*70}")
    print(f"SINGLE MEDIA TRANSFER: {SRC_NAME} -> {DST_NAME}, delay={delay}s")
    print(f"{'='*70}")

    # Start serial monitors on BOTH devices
    stop_event = threading.Event()
    # NOTE: We can't open serial for monitoring AND use meshtastic on the same port.
    # So we only monitor the RECEIVER's serial output via a raw serial connection BEFORE
    # the meshtastic interface opens.
    # Actually, meshtastic uses the serial port exclusively. So we'll just monitor the
    # SOURCE's meshtastic pubsub for responses, and rely on firmware debug logs going
    # to the serial that meshtastic reads.

    # Encode packets
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
    # type=0 (MEDIA_CHUNK) omitted since it's default
    chunk_pkt.extend(enc_fv(2, tid))
    chunk_pkt.extend(enc_fb(6, data))

    complete_pkt = bytearray()
    complete_pkt.extend(enc_fv(1, 2))   # MEDIA_COMPLETE
    complete_pkt.extend(enc_fv(2, tid))
    complete_pkt.extend(enc_fv(9, checksum))

    print(f"  tid=0x{tid:08X}, checksum=0x{checksum:08X}, data=100 bytes")
    print(f"  START  packet: {len(start_pkt)} bytes: {start_pkt.hex()}")
    print(f"  CHUNK  packet: {len(chunk_pkt)} bytes ({len(chunk_pkt)} total)")
    print(f"  COMPLETE packet: {len(complete_pkt)} bytes: {complete_pkt.hex()}")

    # Open meshtastic interface on SOURCE
    ack_event = threading.Event()
    ack_result = [None]
    all_rx = []

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
            # Decode media type from payload
            mtype = "?"
            if isinstance(payload, (bytes, bytearray)) and len(payload) > 0:
                pos = 0
                found_type = None
                while pos < len(payload):
                    tw = 0; shift = 0
                    while pos < len(payload):
                        b = payload[pos]; pos += 1
                        tw |= (b & 0x7F) << shift; shift += 7
                        if not (b & 0x80): break
                    fn = tw >> 3; wt = tw & 7
                    if wt == 0:
                        v = 0; shift = 0
                        while pos < len(payload):
                            b = payload[pos]; pos += 1
                            v |= (b & 0x7F) << shift; shift += 7
                            if not (b & 0x80): break
                        if fn == 1: found_type = v
                        if fn == 2: extra += f" tid=0x{v:08X}"
                    elif wt == 2:
                        ln = 0; shift = 0
                        while pos < len(payload):
                            b = payload[pos]; pos += 1
                            ln |= (b & 0x7F) << shift; shift += 7
                            if not (b & 0x80): break
                        pos += ln
                    else:
                        break
                if found_type is not None:
                    mtype = {0:"CHUNK", 1:"START", 2:"COMPLETE", 3:"NACK", 4:"ACK_COMPLETE", 5:"CANCEL"}.get(found_type, f"?{found_type}")
                else:
                    mtype = "CHUNK(default)"
            extra = f" type={mtype}" + extra

            if mtype in ("ACK_COMPLETE", "NACK"):
                ack_result[0] = {"type": mtype, "from": fromNode}
                ack_event.set()

        msg = f"  [{ts}] RX: from={fromNode} to={toNode} portnum={portnum}{extra}"
        print(msg, flush=True)
        all_rx.append(msg)

    print(f"\n  Opening meshtastic on {SRC_PORT}...", flush=True)
    iface = meshtastic.serial_interface.SerialInterface(SRC_PORT)
    time.sleep(2)
    pub.subscribe(on_rx, "meshtastic.receive")

    # Send packets
    ts = time.strftime("%H:%M:%S")
    print(f"\n  [{ts}] TX: MEDIA_START -> 0x{DST_ID:08x}", flush=True)
    iface.sendData(bytes(start_pkt), destinationId=DST_ID, portNum=259,
                   wantAck=True, wantResponse=False)
    time.sleep(delay)

    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] TX: MEDIA_CHUNK (100B) -> 0x{DST_ID:08x}", flush=True)
    iface.sendData(bytes(chunk_pkt), destinationId=DST_ID, portNum=259,
                   wantAck=True, wantResponse=False)
    time.sleep(delay)

    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] TX: MEDIA_COMPLETE -> 0x{DST_ID:08x}", flush=True)
    iface.sendData(bytes(complete_pkt), destinationId=DST_ID, portNum=259,
                   wantAck=True, wantResponse=False)

    print(f"\n  Waiting 60s for ACK_COMPLETE or NACK...", flush=True)
    ack_event.wait(timeout=60)

    if ack_result[0]:
        r = ack_result[0]
        if r["type"] == "ACK_COMPLETE":
            print(f"\n  *** SUCCESS: ACK_COMPLETE from {r['from']} ***", flush=True)
        elif r["type"] == "NACK":
            print(f"\n  *** NACK from {r['from']} ***", flush=True)
        else:
            print(f"\n  *** Response: {r['type']} from {r['from']} ***", flush=True)
    else:
        print(f"\n  *** NO media response in 60s ***", flush=True)

    # Wait a bit more to catch any late responses
    time.sleep(5)

    try: pub.unsubscribe(on_rx, "meshtastic.receive")
    except: pass
    iface.close()

    print(f"\n{'='*70}")
    print(f"TOTAL RX EVENTS: {len(all_rx)}")
    print(f"{'='*70}")

if __name__ == "__main__":
    main()
