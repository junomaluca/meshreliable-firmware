#!/usr/bin/env python3
"""Diagnostic: test sendData() with both text and media portnums.
Opens on VHF-A, sends to VHF-B, logs all received packets with detail."""
import sys, time, random, threading
import meshtastic
import meshtastic.serial_interface
from pubsub import pub

SRC_PORT = "/dev/cu.usbmodem101"   # VHF-A
DST_ID = 861805532                  # VHF-B

received = []

def on_receive(packet, interface):
    decoded = packet.get("decoded", {})
    portnum = decoded.get("portnum", "?")
    fromNode = packet.get("fromId", "?")
    toNode = packet.get("toId", "?")
    payload = decoded.get("payload", b"")

    ts = time.strftime("%H:%M:%S")
    extra = ""

    # Decode ROUTING_APP payload
    if portnum == "ROUTING_APP":
        routing = decoded.get("routing", {})
        if routing:
            extra = f" routing={routing}"
        elif isinstance(payload, (bytes, bytearray)):
            extra = f" raw={payload.hex()}"
    elif portnum == "TEXT_MESSAGE_APP":
        if isinstance(payload, (bytes, bytearray)):
            extra = f" text={payload.decode('utf-8', errors='replace')}"
    elif portnum in (259, "MEDIA_TRANSFER_APP"):
        extra = f" *** MEDIA PACKET ***"

    print(f"  [{ts}] RX: from={fromNode} to={toNode} portnum={portnum}{extra}", flush=True)
    received.append(packet)

print(f"Opening SerialInterface on {SRC_PORT}...", flush=True)
iface = meshtastic.serial_interface.SerialInterface(SRC_PORT)
time.sleep(2)
pub.subscribe(on_receive, "meshtastic.receive")

# Test 1: sendData with TEXT_MESSAGE_APP (should work like CLI)
print(f"\n--- Test 1: sendData TEXT_MESSAGE_APP ---", flush=True)
text_payload = f"api_test_{int(time.time())&0xFFFF}".encode("utf-8")
iface.sendData(text_payload, destinationId=DST_ID, portNum=1,  # TEXT_MESSAGE_APP = 1
               wantAck=True, wantResponse=False)
print(f"  Sent {len(text_payload)}B text via sendData()", flush=True)
time.sleep(15)

# Test 2: sendData with MEDIA_TRANSFER_APP
print(f"\n--- Test 2: sendData MEDIA_TRANSFER_APP (START only) ---", flush=True)
# Minimal MEDIA_START protobuf
def enc_varint(v):
    buf = bytearray()
    if v == 0: buf.append(0); return buf
    while v > 0x7F: buf.append((v & 0x7F) | 0x80); v >>= 7
    buf.append(v & 0x7F)
    return buf
def enc_fv(fn, val):
    if val == 0: return bytearray()
    return enc_varint((fn << 3) | 0) + enc_varint(val)

tid = random.randint(0x1000, 0xFFFFFFF)
start_pkt = bytearray()
start_pkt.extend(enc_fv(1, 1))     # type = MEDIA_START
start_pkt.extend(enc_fv(2, tid))
start_pkt.extend(enc_fv(4, 1))     # total_chunks
start_pkt.extend(enc_fv(5, 100))   # total_size

iface.sendData(bytes(start_pkt), destinationId=DST_ID, portNum=259,
               wantAck=True, wantResponse=False)
print(f"  Sent MEDIA_START {len(start_pkt)}B, tid=0x{tid:08X}", flush=True)
time.sleep(15)

# Test 3: sendData with wantAck=False
print(f"\n--- Test 3: sendData MEDIA_TRANSFER_APP wantAck=False ---", flush=True)
tid2 = random.randint(0x1000, 0xFFFFFFF)
start_pkt2 = bytearray()
start_pkt2.extend(enc_fv(1, 1))     # type = MEDIA_START
start_pkt2.extend(enc_fv(2, tid2))
start_pkt2.extend(enc_fv(4, 1))
start_pkt2.extend(enc_fv(5, 100))

iface.sendData(bytes(start_pkt2), destinationId=DST_ID, portNum=259,
               wantAck=False, wantResponse=False)
print(f"  Sent MEDIA_START {len(start_pkt2)}B (no ack), tid=0x{tid2:08X}", flush=True)
time.sleep(15)

print(f"\n{'='*60}", flush=True)
print(f"Total received: {len(received)} packets", flush=True)
portnums = [p.get("decoded",{}).get("portnum","?") for p in received]
print(f"Portnums: {portnums}", flush=True)

try: pub.unsubscribe(on_receive, "meshtastic.receive")
except: pass
iface.close()
