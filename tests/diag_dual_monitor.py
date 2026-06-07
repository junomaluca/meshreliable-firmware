#!/usr/bin/env python3
"""Diagnostic: open meshtastic on BOTH devices simultaneously.
Send from VHF-B, listen on VHF-A to see what the receiver sees."""
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

def decode_payload_type(payload):
    """Decode the type field from a MediaTransfer protobuf payload."""
    if not isinstance(payload, (bytes, bytearray)) or len(payload) == 0:
        return "?", {}
    pos = 0
    found = {}
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
            found[fn] = v
        elif wt == 2:
            ln = 0; shift = 0
            while pos < len(payload):
                b = payload[pos]; pos += 1
                ln |= (b & 0x7F) << shift; shift += 7
                if not (b & 0x80): break
            found[f"f{fn}_bytes"] = ln
            pos += ln
        else:
            break
    type_val = found.get(1, 0)  # default 0 = CHUNK
    type_name = {0:"CHUNK", 1:"START", 2:"COMPLETE", 3:"NACK", 4:"ACK_COMPLETE", 5:"CANCEL"}.get(type_val, f"?{type_val}")
    return type_name, found

def make_listener(label, results_list, ack_event, ack_result):
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
            mtype, fields = decode_payload_type(payload)
            extra = f" type={mtype} fields={fields}"
            if mtype in ("ACK_COMPLETE", "NACK"):
                ack_result[0] = {"type": mtype, "from": fromNode, "to": toNode}
                ack_event.set()

        msg = f"  [{ts}] {label}: from={fromNode} to={toNode} port={portnum}{extra}"
        print(msg, flush=True)
        results_list.append(msg)
    return on_rx

def main():
    delay = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    print(f"{'='*70}")
    print(f"DUAL MONITOR: Send VHF-B -> VHF-A, delay={delay}s")
    print(f"Monitoring meshtastic on BOTH devices simultaneously")
    print(f"{'='*70}\n")

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
    chunk_pkt.extend(enc_fv(2, tid))    # type=0 CHUNK omitted (default)
    chunk_pkt.extend(enc_fb(6, data))

    complete_pkt = bytearray()
    complete_pkt.extend(enc_fv(1, 2))   # MEDIA_COMPLETE
    complete_pkt.extend(enc_fv(2, tid))
    complete_pkt.extend(enc_fv(9, checksum))

    print(f"  tid=0x{tid:08X}, checksum=0x{checksum:08X}")
    print(f"  START:    {start_pkt.hex()}")
    print(f"  CHUNK:    {len(chunk_pkt)} bytes")
    print(f"  COMPLETE: {complete_pkt.hex()}\n")

    # Open RECEIVER first
    print(f"  Opening VHF-A (receiver) on {VHF_A_PORT}...", flush=True)
    iface_a = meshtastic.serial_interface.SerialInterface(VHF_A_PORT)
    time.sleep(2)

    rx_a = []
    ack_event = threading.Event()
    ack_result = [None]
    listener_a = make_listener("VHF-A-RX", rx_a, ack_event, ack_result)
    pub.subscribe(listener_a, "meshtastic.receive")

    # Open SENDER
    print(f"  Opening VHF-B (sender) on {VHF_B_PORT}...", flush=True)
    iface_b = meshtastic.serial_interface.SerialInterface(VHF_B_PORT)
    time.sleep(2)

    rx_b = []
    listener_b = make_listener("VHF-B-RX", rx_b, ack_event, ack_result)
    pub.subscribe(listener_b, "meshtastic.receive")

    # Send packets from VHF-B
    ts = time.strftime("%H:%M:%S")
    print(f"\n  [{ts}] TX(VHF-B): MEDIA_START -> VHF-A", flush=True)
    iface_b.sendData(bytes(start_pkt), destinationId=VHF_A_ID, portNum=259,
                     wantAck=True, wantResponse=False)
    time.sleep(delay)

    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] TX(VHF-B): MEDIA_CHUNK (100B) -> VHF-A", flush=True)
    iface_b.sendData(bytes(chunk_pkt), destinationId=VHF_A_ID, portNum=259,
                     wantAck=True, wantResponse=False)
    time.sleep(delay)

    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] TX(VHF-B): MEDIA_COMPLETE -> VHF-A", flush=True)
    iface_b.sendData(bytes(complete_pkt), destinationId=VHF_A_ID, portNum=259,
                     wantAck=True, wantResponse=False)

    print(f"\n  Waiting 90s for ACK_COMPLETE or NACK...", flush=True)
    ack_event.wait(timeout=90)

    if ack_result[0]:
        r = ack_result[0]
        print(f"\n  *** RESULT: {r['type']} from={r['from']} to={r['to']} ***", flush=True)
    else:
        print(f"\n  *** NO media response in 90s ***", flush=True)

    time.sleep(3)

    try: pub.unsubscribe(listener_a, "meshtastic.receive")
    except: pass
    try: pub.unsubscribe(listener_b, "meshtastic.receive")
    except: pass

    iface_a.close()
    iface_b.close()

    print(f"\n{'='*70}")
    print(f"VHF-A received {len(rx_a)} events")
    print(f"VHF-B received {len(rx_b)} events")
    print(f"{'='*70}")

if __name__ == "__main__":
    main()
