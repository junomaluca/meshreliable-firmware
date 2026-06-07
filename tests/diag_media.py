#!/usr/bin/env python3
"""Diagnostic: test media transfer ACK mechanism with detailed logging."""
import sys, time, random, struct, threading
import meshtastic
import meshtastic.serial_interface
from pubsub import pub

SRC_PORT = "/dev/cu.usbmodem101"   # VHF-A (sender)
DST_ID = 861805532                  # VHF-B node ID (destination)
PORTNUM = 259  # MEDIA_TRANSFER_APP

# Protobuf encoding helpers (minimal)
def encode_varint(value):
    buf = bytearray()
    if value == 0: buf.append(0); return buf
    while value > 0x7F: buf.append((value & 0x7F) | 0x80); value >>= 7
    buf.append(value & 0x7F)
    return buf
def encode_fv(fn, val):
    if val == 0: return bytearray()
    return encode_varint((fn << 3) | 0) + encode_varint(val)
def encode_fb(fn, val):
    if not val: return bytearray()
    return encode_varint((fn << 3) | 2) + encode_varint(len(val)) + bytearray(val)
def crc32(data):
    crc = 0xFFFFFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ 0xEDB88320 if crc & 1 else crc >> 1
    return (~crc) & 0xFFFFFFFF

# Build a minimal media transfer: 100 bytes = 1 chunk
data = bytes([0x42] * 100)
checksum = crc32(data)
tid = random.randint(0x1000, 0xFFFFFFF)

start_pkt = bytearray()
start_pkt.extend(encode_fv(1, 1))     # type = MEDIA_START
start_pkt.extend(encode_fv(2, tid))   # transfer_id
start_pkt.extend(encode_fv(4, 1))     # total_chunks = 1
start_pkt.extend(encode_fv(5, 100))   # total_size = 100
start_pkt.extend(encode_fv(7, 0))     # content_type = VOICE_MEMO
start_pkt.extend(encode_fv(9, checksum))

chunk_pkt = bytearray()
chunk_pkt.extend(encode_fv(1, 0))     # type = MEDIA_CHUNK (0 won't encode, which is correct)
chunk_pkt.extend(encode_fv(2, tid))   # transfer_id
chunk_pkt.extend(encode_fv(3, 0))     # chunk_index = 0 (won't encode, correct)
chunk_pkt.extend(encode_fb(6, data))  # chunk_data

complete_pkt = bytearray()
complete_pkt.extend(encode_fv(1, 2))  # type = MEDIA_COMPLETE
complete_pkt.extend(encode_fv(2, tid))
complete_pkt.extend(encode_fv(9, checksum))

print(f"Transfer ID: 0x{tid:08X}", flush=True)
print(f"Data: {len(data)} bytes, checksum: 0x{checksum:08X}", flush=True)
print(f"Packets: START={len(start_pkt)}B, CHUNK={len(chunk_pkt)}B, COMPLETE={len(complete_pkt)}B", flush=True)
print(flush=True)

# Track ALL received packets
all_received = []
ack_event = threading.Event()
ack_result = [None]

def on_receive_all(packet, interface):
    decoded = packet.get("decoded", {})
    portnum = decoded.get("portnum", "?")
    fromNode = packet.get("fromId", "?")
    toNode = packet.get("toId", "?")
    payload = decoded.get("payload", b"")
    payload_len = len(payload) if isinstance(payload, (bytes, bytearray)) else 0
    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] RX: from={fromNode} to={toNode} portnum={portnum} "
          f"payload={payload_len}B", flush=True)
    all_received.append(packet)

    # Check for media ACK/NACK
    if portnum in (PORTNUM, 259, "MEDIA_TRANSFER_APP"):
        print(f"  *** MEDIA PACKET RECEIVED! portnum={portnum} ***", flush=True)
        if isinstance(payload, (bytes, bytearray)) and len(payload) > 0:
            # Try to decode type field
            if len(payload) >= 2:
                # First varint should be field 1 (type)
                pos = 0
                while pos < len(payload) and pos < 5:
                    b = payload[pos]
                    print(f"    byte[{pos}]: 0x{b:02X}", flush=True)
                    pos += 1
        ack_result[0] = packet
        ack_event.set()

print(f"Opening SerialInterface on {SRC_PORT} (source device)...", flush=True)
iface = meshtastic.serial_interface.SerialInterface(SRC_PORT)
time.sleep(2)

pub.subscribe(on_receive_all, "meshtastic.receive")
print(f"Subscribed to meshtastic.receive", flush=True)
print(flush=True)

# Send START
print(f"Sending MEDIA_START ({len(start_pkt)}B)...", flush=True)
iface.sendData(bytes(start_pkt), destinationId=DST_ID, portNum=PORTNUM,
               wantAck=True, wantResponse=False)
print(f"  MEDIA_START sent", flush=True)
time.sleep(5)

# Send CHUNK
print(f"Sending MEDIA_CHUNK ({len(chunk_pkt)}B)...", flush=True)
iface.sendData(bytes(chunk_pkt), destinationId=DST_ID, portNum=PORTNUM,
               wantAck=True, wantResponse=False)
print(f"  MEDIA_CHUNK sent", flush=True)
time.sleep(5)

# Send COMPLETE
print(f"Sending MEDIA_COMPLETE ({len(complete_pkt)}B)...", flush=True)
iface.sendData(bytes(complete_pkt), destinationId=DST_ID, portNum=PORTNUM,
               wantAck=True, wantResponse=False)
print(f"  MEDIA_COMPLETE sent", flush=True)
print(flush=True)

# Wait for ACK
print(f"Waiting up to 90s for ACK_COMPLETE/NACK...", flush=True)
ack_event.wait(timeout=90)

print(f"\n{'='*60}", flush=True)
print(f"Total packets received: {len(all_received)}", flush=True)
if ack_result[0]:
    print(f"MEDIA RESPONSE RECEIVED!", flush=True)
else:
    print(f"NO MEDIA RESPONSE received in 90s", flush=True)
print(f"All received portnums: {[p.get('decoded',{}).get('portnum','?') for p in all_received]}", flush=True)

try:
    pub.unsubscribe(on_receive_all, "meshtastic.receive")
except:
    pass
iface.close()
