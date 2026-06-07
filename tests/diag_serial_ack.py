#!/usr/bin/env python3
"""Capture VHF-B raw serial debug output while sending media from VHF-A.
Uses separate serial reader thread for VHF-B debug output."""
import sys, time, random, threading, serial
import meshtastic
import meshtastic.serial_interface

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

    # Open VHF-A via meshtastic (for sending)
    print("Opening VHF-A sender...", flush=True)
    iface_a = meshtastic.serial_interface.SerialInterface(VHF_A_PORT)
    time.sleep(3)

    print("Sending media packets to VHF-B...")
    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] TX: START", flush=True)
    iface_a.sendData(bytes(start_pkt), destinationId=VHF_B_ID, portNum=259,
                     wantAck=False, wantResponse=False)
    time.sleep(10)

    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] TX: CHUNK", flush=True)
    iface_a.sendData(bytes(chunk_pkt), destinationId=VHF_B_ID, portNum=259,
                     wantAck=False, wantResponse=False)
    time.sleep(10)

    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] TX: COMPLETE", flush=True)
    iface_a.sendData(bytes(complete_pkt), destinationId=VHF_B_ID, portNum=259,
                     wantAck=False, wantResponse=False)

    print("  Waiting 60s for ACK_COMPLETE...", flush=True)
    time.sleep(60)

    iface_a.close()

    # Now read VHF-B's rebootCount to check if it crashed
    print("\nChecking VHF-B for crash (rebootCount)...")
    try:
        iface_b = meshtastic.serial_interface.SerialInterface(VHF_B_PORT)
        time.sleep(2)
        node = iface_b.getMyNodeInfo()
        reboot = node.get('deviceMetrics', {}).get('rebootCount', '?')
        uptime = node.get('deviceMetrics', {}).get('uptimeSeconds', '?')
        print(f"  VHF-B rebootCount={reboot}, uptime={uptime}s")
        iface_b.close()
    except Exception as e:
        print(f"  VHF-B check failed: {e}")

if __name__ == "__main__":
    main()
