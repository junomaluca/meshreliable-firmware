#!/usr/bin/env python3
"""Diagnostic: test if meshtastic pubsub delivers incoming radio packets."""
import sys, time, threading
import meshtastic
import meshtastic.serial_interface
from pubsub import pub

PORT = "/dev/cu.usbmodem1101"  # VHF-B
received = []

def on_receive(packet, interface):
    portnum = packet.get("decoded", {}).get("portnum", "?")
    fromNode = packet.get("fromId", "?")
    toNode = packet.get("toId", "?")
    print(f"  RX: from={fromNode} to={toNode} portnum={portnum}", flush=True)
    received.append(packet)

print(f"Opening SerialInterface on {PORT}...", flush=True)
iface = meshtastic.serial_interface.SerialInterface(PORT)
time.sleep(2)

pub.subscribe(on_receive, "meshtastic.receive")
print(f"Listening for ALL packets for 30s...", flush=True)
print(f"(Send a text from another device to generate traffic)", flush=True)

try:
    time.sleep(30)
except KeyboardInterrupt:
    pass

print(f"\nReceived {len(received)} packets total", flush=True)
for p in received:
    decoded = p.get("decoded", {})
    print(f"  portnum={decoded.get('portnum','?')} from={p.get('fromId','?')} "
          f"payload_len={len(decoded.get('payload', b''))}", flush=True)

pub.unsubAll()
iface.close()
