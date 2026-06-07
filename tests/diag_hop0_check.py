#!/usr/bin/env python3
"""Check if hop_limit=0 ACK_COMPLETE is sent by capturing receiver serial log."""
import sys, time, random, threading, subprocess, os
import meshtastic
import meshtastic.serial_interface
from pubsub import pub

VHF_A_PORT = "/dev/cu.usbmodem101"
VHF_B_PORT = "/dev/cu.usbmodem1101"
VHF_A_ID   = 0x335e1be8
VHF_B_ID   = 0x335e1bdc
MESHTASTIC = "/Users/patrick/Library/Python/3.14/bin/meshtastic"

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

def main():
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

    print(f"tid=0x{tid:08X}")

    # Capture VHF-B serial (receiver) while sending from VHF-A
    logfile = "/tmp/vhfb_serial.log"
    print(f"Starting VHF-B serial log -> {logfile}")
    log_proc = subprocess.Popen(
        [MESHTASTIC, "--port", VHF_B_PORT, "--seriallog", logfile, "--listen"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    time.sleep(5)

    # Send from VHF-A
    print("Opening VHF-A sender...")
    iface_a = meshtastic.serial_interface.SerialInterface(VHF_A_PORT)
    time.sleep(2)

    print("TX: START"); iface_a.sendData(bytes(start_pkt), destinationId=VHF_B_ID, portNum=259, wantAck=False)
    time.sleep(5)
    print("TX: CHUNK"); iface_a.sendData(bytes(chunk_pkt), destinationId=VHF_B_ID, portNum=259, wantAck=False)
    time.sleep(5)
    print("TX: COMPLETE"); iface_a.sendData(bytes(complete_pkt), destinationId=VHF_B_ID, portNum=259, wantAck=False)
    print("Waiting 30s...")
    time.sleep(30)

    iface_a.close()
    log_proc.terminate()
    time.sleep(2)

    print(f"\n{'='*70}")
    print("VHF-B SERIAL LOG (MediaXfer/ACK/send/hop entries):")
    print(f"{'='*70}")
    if os.path.exists(logfile):
        with open(logfile, 'r', errors='replace') as f:
            for line in f:
                line = line.strip()
                if any(k in line.lower() for k in ['mediaxfer', 'ack_complete', 'nack', 'sending', 'error decoding',
                                                     'proto module', 'hop', 'sendlocal', 'rawsend', 'queue',
                                                     'portnum=259', 'mediaxfer', 'mesh send']):
                    print(f"  {line}")

if __name__ == "__main__":
    main()
