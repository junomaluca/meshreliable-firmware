#!/usr/bin/env python3
"""Diagnostic: Monitor VHF-A serial logs for MediaXfer debug output
while sending media transfer from VHF-B.
Uses meshtastic CLI --seriallog on receiver, Python sendData on sender."""
import sys, time, random, threading, subprocess, os
import meshtastic
import meshtastic.serial_interface
from pubsub import pub

VHF_A_PORT = "/dev/cu.usbmodem101"
VHF_B_PORT = "/dev/cu.usbmodem1101"
VHF_A_ID   = 0x335e1be8
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
    delay = 10

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
    chunk_pkt.extend(enc_fv(2, tid))    # type=0 CHUNK omitted
    chunk_pkt.extend(enc_fb(6, data))

    complete_pkt = bytearray()
    complete_pkt.extend(enc_fv(1, 2))   # MEDIA_COMPLETE
    complete_pkt.extend(enc_fv(2, tid))
    complete_pkt.extend(enc_fv(9, checksum))

    print(f"tid=0x{tid:08X}, checksum=0x{checksum:08X}")

    # Start VHF-A serial log capture (captures firmware debug output)
    logfile = "/tmp/vhfa_serial.log"
    print(f"Starting VHF-A serial log capture -> {logfile}")
    log_proc = subprocess.Popen(
        [MESHTASTIC, "--port", VHF_A_PORT, "--seriallog", logfile, "--listen"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    time.sleep(5)  # Let it connect

    # Open sender on VHF-B
    print(f"Opening VHF-B sender...")
    ack_event = threading.Event()
    ack_result = [None]

    def on_rx(packet, interface):
        decoded = packet.get("decoded", {})
        portnum = decoded.get("portnum", "?")
        if portnum in (259, "MEDIA_TRANSFER_APP"):
            payload = decoded.get("payload", b"")
            ack_result[0] = payload
            ack_event.set()
            print(f"  RX portnum=259 payload={len(payload)}B")

    iface_b = meshtastic.serial_interface.SerialInterface(VHF_B_PORT)
    time.sleep(2)
    pub.subscribe(on_rx, "meshtastic.receive")

    # Send
    print(f"\nSending MEDIA_START...")
    iface_b.sendData(bytes(start_pkt), destinationId=VHF_A_ID, portNum=259,
                     wantAck=True, wantResponse=False)
    time.sleep(delay)

    print(f"Sending MEDIA_CHUNK (100B)...")
    iface_b.sendData(bytes(chunk_pkt), destinationId=VHF_A_ID, portNum=259,
                     wantAck=True, wantResponse=False)
    time.sleep(delay)

    print(f"Sending MEDIA_COMPLETE...")
    iface_b.sendData(bytes(complete_pkt), destinationId=VHF_A_ID, portNum=259,
                     wantAck=True, wantResponse=False)

    print(f"\nWaiting 60s...")
    ack_event.wait(timeout=60)

    if ack_result[0]:
        print(f"Got media response!")
    else:
        print(f"No media response in 60s")

    try: pub.unsubscribe(on_rx, "meshtastic.receive")
    except: pass
    iface_b.close()

    # Stop VHF-A log capture
    log_proc.terminate()
    time.sleep(2)

    # Read and display relevant log lines
    print(f"\n{'='*70}")
    print(f"VHF-A SERIAL LOG (MediaXfer/decode entries):")
    print(f"{'='*70}")
    if os.path.exists(logfile):
        with open(logfile, 'r', errors='replace') as f:
            for line in f:
                line = line.strip()
                if any(k in line for k in ['MediaXfer', 'media', 'Error decoding', 'proto module',
                                            'portnum=259', 'MEDIA', 'pb_decode', 'handleReceived']):
                    print(f"  {line}")
    else:
        print(f"  (log file not found)")

if __name__ == "__main__":
    main()
