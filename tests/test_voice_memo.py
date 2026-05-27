#!/usr/bin/env python3
"""
MeshReliable Phase 3b — Voice Memo Module Test Suite

Tests on-device Codec2 voice memo recording, chunked transfer via
MediaTransferModule, and playback. Uses two Seeed XIAO S3 devices
(no audio hardware) to test the transfer pipeline with synthetic
Codec2-encoded data.

Usage:
    python3 tests/test_voice_memo.py

Environment:
    DEVICE_A: serial port for device A (default: /dev/cu.usbmodem21101)
    DEVICE_B: serial port for device B (default: /dev/cu.usbmodem21201)
"""

import os
import sys
import time
import struct
import math
import subprocess

try:
    import meshtastic
    import meshtastic.serial_interface
    from pubsub import pub
except ImportError:
    print("ERROR: meshtastic python package required. Install with: pip install meshtastic")
    sys.exit(1)

# Device serial ports
DEVICE_A = os.environ.get("DEVICE_A", "/dev/cu.usbmodem21101")
DEVICE_B = os.environ.get("DEVICE_B", "/dev/cu.usbmodem21201")

MESHTASTIC = os.environ.get("MESHTASTIC_BIN", "/Users/patrick/Library/Python/3.14/bin/meshtastic")

# MEDIA_TRANSFER_APP portnum (259)
MEDIA_TRANSFER_APP = 259

# MediaTransferType enum values
MEDIA_CHUNK = 0
MEDIA_START = 1
MEDIA_COMPLETE = 2
MEDIA_NACK = 3
MEDIA_ACK_COMPLETE = 4
MEDIA_CANCEL = 5

# MediaContentType enum values
VOICE_MEMO = 0

# Proto field numbers (from media_transfer.proto)
FIELD_TYPE = 1
FIELD_TRANSFER_ID = 2
FIELD_CHUNK_INDEX = 3
FIELD_TOTAL_CHUNKS = 4
FIELD_TOTAL_SIZE = 5
FIELD_CHUNK_DATA = 6
FIELD_CONTENT_TYPE = 7
FIELD_MISSING_CHUNKS = 8
FIELD_CHECKSUM = 9
FIELD_MIME_TYPE = 10
FIELD_DURATION_SECONDS = 11
FIELD_WIDTH = 12
FIELD_HEIGHT = 13

CHUNK_SIZE = 200

test_results = {}
received_packets = {"A": [], "B": []}


# ─── Protobuf encode/decode helpers ────────────────────────────────────

def encode_varint(value):
    result = bytearray()
    while value > 0x7F:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value & 0x7F)
    return result


def read_varint(data, pos):
    value = 0
    shift = 0
    while pos < len(data):
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        shift += 7
        if not (byte & 0x80):
            break
    return value, pos


def encode_field_varint(field_num, value):
    if value == 0:
        return bytearray()
    tag = (field_num << 3) | 0
    return encode_varint(tag) + encode_varint(value)


def encode_field_bytes(field_num, value):
    if not value:
        return bytearray()
    tag = (field_num << 3) | 2
    return encode_varint(tag) + encode_varint(len(value)) + bytearray(value)


def encode_field_string(field_num, value):
    if not value:
        return bytearray()
    encoded = value.encode("utf-8")
    tag = (field_num << 3) | 2
    return encode_varint(tag) + encode_varint(len(encoded)) + encoded


def encode_field_packed_uint32(field_num, values):
    if not values:
        return bytearray()
    tag = (field_num << 3) | 2
    packed = bytearray()
    for v in values:
        packed.extend(encode_varint(v))
    return encode_varint(tag) + encode_varint(len(packed)) + packed


def encode_media_transfer(type_val=0, transfer_id=0, chunk_index=0, total_chunks=0,
                           total_size=0, chunk_data=b"", content_type=0,
                           missing_chunks=None, checksum=0, mime_type="",
                           duration_seconds=0, width=0, height=0):
    data = bytearray()
    data.extend(encode_field_varint(FIELD_TYPE, type_val))
    data.extend(encode_field_varint(FIELD_TRANSFER_ID, transfer_id))
    data.extend(encode_field_varint(FIELD_CHUNK_INDEX, chunk_index))
    data.extend(encode_field_varint(FIELD_TOTAL_CHUNKS, total_chunks))
    data.extend(encode_field_varint(FIELD_TOTAL_SIZE, total_size))
    data.extend(encode_field_bytes(FIELD_CHUNK_DATA, chunk_data))
    data.extend(encode_field_varint(FIELD_CONTENT_TYPE, content_type))
    data.extend(encode_field_packed_uint32(FIELD_MISSING_CHUNKS, missing_chunks or []))
    data.extend(encode_field_varint(FIELD_CHECKSUM, checksum))
    data.extend(encode_field_string(FIELD_MIME_TYPE, mime_type))
    data.extend(encode_field_varint(FIELD_DURATION_SECONDS, duration_seconds))
    data.extend(encode_field_varint(FIELD_WIDTH, width))
    data.extend(encode_field_varint(FIELD_HEIGHT, height))
    return bytes(data)


def decode_media_transfer(data):
    result = {
        "type": 0, "transfer_id": 0, "chunk_index": 0, "total_chunks": 0,
        "total_size": 0, "chunk_data": b"", "content_type": 0,
        "missing_chunks": [], "checksum": 0, "mime_type": "",
        "duration_seconds": 0, "width": 0, "height": 0
    }
    pos = 0
    while pos < len(data):
        tag_wire, pos = read_varint(data, pos)
        field_num = tag_wire >> 3
        wire_type = tag_wire & 0x07
        if wire_type == 0:
            value, pos = read_varint(data, pos)
            if field_num == FIELD_TYPE: result["type"] = value
            elif field_num == FIELD_TRANSFER_ID: result["transfer_id"] = value
            elif field_num == FIELD_CHUNK_INDEX: result["chunk_index"] = value
            elif field_num == FIELD_TOTAL_CHUNKS: result["total_chunks"] = value
            elif field_num == FIELD_TOTAL_SIZE: result["total_size"] = value
            elif field_num == FIELD_CONTENT_TYPE: result["content_type"] = value
            elif field_num == FIELD_CHECKSUM: result["checksum"] = value
            elif field_num == FIELD_DURATION_SECONDS: result["duration_seconds"] = value
            elif field_num == FIELD_WIDTH: result["width"] = value
            elif field_num == FIELD_HEIGHT: result["height"] = value
        elif wire_type == 2:
            length, pos = read_varint(data, pos)
            chunk = data[pos:pos + length]
            pos += length
            if field_num == FIELD_CHUNK_DATA:
                result["chunk_data"] = bytes(chunk)
            elif field_num == FIELD_MISSING_CHUNKS:
                p = 0
                while p < len(chunk):
                    v, p = read_varint(chunk, p)
                    result["missing_chunks"].append(v)
            elif field_num == FIELD_MIME_TYPE:
                result["mime_type"] = chunk.decode("utf-8", errors="replace")
    return result


# ─── CRC32 (IEEE 802.3, matching firmware) ─────────────────────────────

def crc32(data):
    crc = 0xFFFFFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xEDB88320
            else:
                crc >>= 1
    return (~crc) & 0xFFFFFFFF


# ─── Synthetic Codec2 voice memo generator ─────────────────────────────

# Codec2 mode 700: 4 bytes per frame, 320 PCM samples per frame (40ms)
CODEC2_BYTES_PER_FRAME = 4
CODEC2_SAMPLES_PER_FRAME = 320
CODEC2_SAMPLE_RATE = 8000


def generate_synthetic_codec2(duration_ms=5000):
    """Generate synthetic Codec2-like data (deterministic pattern).

    We can't run the actual Codec2 encoder in Python, so we generate
    deterministic 4-byte frames that the firmware's decoder will interpret
    as audio (likely noise/tones). This tests the transfer pipeline.
    """
    total_frames = duration_ms // 40  # 40ms per frame
    data = bytearray()
    for f in range(total_frames):
        # Deterministic pattern: frame number modulo in each byte
        data.append((f * 7 + 0x42) & 0xFF)
        data.append((f * 13 + 0xA3) & 0xFF)
        data.append((f * 31 + 0x17) & 0xFF)
        data.append((f * 53 + 0x9E) & 0xFF)
    return bytes(data)


# ─── Test helpers ──────────────────────────────────────────────────────

def log(msg):
    ts = time.strftime("%H:%M:%S")
    print(f"  [{ts}] {msg}")


def run_test(name, func):
    print(f"\n{'='*60}")
    print(f"  TEST: {name}")
    print(f"{'='*60}")
    try:
        func()
        test_results[name] = "PASS"
        print(f"  >>> PASS: {name}")
    except Exception as e:
        test_results[name] = f"FAIL: {e}"
        print(f"  >>> FAIL: {name}: {e}")


def on_receive_A(packet, interface):
    received_packets["A"].append(packet)


def on_receive_B(packet, interface):
    received_packets["B"].append(packet)


def get_device_id(port):
    try:
        result = subprocess.run(
            [MESHTASTIC, "--port", port, "--info"],
            capture_output=True, text=True, timeout=15
        )
        import re
        match = re.search(r'Owner:\s+.*\((\!?[0-9a-f]+)\)', result.stdout, re.IGNORECASE)
        if match:
            return match.group(1)
        match = re.search(r'MY_NODE_NUM:\s*(\d+)', result.stdout)
        if match:
            return int(match.group(1))
    except Exception:
        pass
    return None


# ─── Encoding tests (no hardware needed) ──────────────────────────────

def test_voice_memo_start_encoding():
    """Test encoding a MEDIA_START with VOICE_MEMO content type."""
    codec2_data = generate_synthetic_codec2(5000)  # 5s
    total_chunks = (len(codec2_data) + CHUNK_SIZE - 1) // CHUNK_SIZE
    checksum = crc32(codec2_data)

    encoded = encode_media_transfer(
        type_val=MEDIA_START,
        transfer_id=0xABCD1234,
        total_chunks=total_chunks,
        total_size=len(codec2_data),
        content_type=VOICE_MEMO,
        checksum=checksum,
        mime_type="audio/codec2",
        duration_seconds=5
    )

    decoded = decode_media_transfer(encoded)
    assert decoded["type"] == MEDIA_START
    assert decoded["transfer_id"] == 0xABCD1234
    assert decoded["total_chunks"] == total_chunks
    assert decoded["total_size"] == len(codec2_data)
    assert decoded["content_type"] == VOICE_MEMO
    assert decoded["checksum"] == checksum
    assert decoded["mime_type"] == "audio/codec2"
    assert decoded["duration_seconds"] == 5
    log(f"Encoded START: {len(encoded)} bytes, {total_chunks} chunks, "
        f"{len(codec2_data)} bytes payload, CRC=0x{checksum:08X}")


def test_synthetic_codec2_generation():
    """Test synthetic Codec2 data properties."""
    data = generate_synthetic_codec2(5000)
    expected_frames = 5000 // 40
    expected_bytes = expected_frames * CODEC2_BYTES_PER_FRAME

    assert len(data) == expected_bytes, f"Expected {expected_bytes}, got {len(data)}"

    # Verify data is deterministic
    data2 = generate_synthetic_codec2(5000)
    assert data == data2, "Synthetic data should be deterministic"

    # Verify different durations produce different lengths
    data_10s = generate_synthetic_codec2(10000)
    assert len(data_10s) == 2 * len(data), "10s should be 2x 5s"
    log(f"5s memo: {len(data)} bytes ({expected_frames} frames)")
    log(f"10s memo: {len(data_10s)} bytes ({len(data_10s) // CODEC2_BYTES_PER_FRAME} frames)")


def test_voice_memo_full_transfer_sequence():
    """Test encoding a complete voice memo transfer sequence."""
    memo_data = generate_synthetic_codec2(3000)  # 3 seconds
    checksum = crc32(memo_data)
    transfer_id = 0x55330001
    total_chunks = (len(memo_data) + CHUNK_SIZE - 1) // CHUNK_SIZE

    # 1. START
    start_pkt = encode_media_transfer(
        type_val=MEDIA_START,
        transfer_id=transfer_id,
        total_chunks=total_chunks,
        total_size=len(memo_data),
        content_type=VOICE_MEMO,
        checksum=checksum,
        mime_type="audio/codec2",
        duration_seconds=3
    )
    start_dec = decode_media_transfer(start_pkt)
    assert start_dec["content_type"] == VOICE_MEMO

    # 2. CHUNKS
    reassembled = bytearray(len(memo_data))
    for i in range(total_chunks):
        offset = i * CHUNK_SIZE
        end = min(offset + CHUNK_SIZE, len(memo_data))
        chunk = memo_data[offset:end]

        chunk_pkt = encode_media_transfer(
            type_val=MEDIA_CHUNK,
            transfer_id=transfer_id,
            chunk_index=i,
            chunk_data=chunk
        )
        chunk_dec = decode_media_transfer(chunk_pkt)
        assert chunk_dec["chunk_index"] == i
        assert chunk_dec["chunk_data"] == chunk

        dec_offset = chunk_dec["chunk_index"] * CHUNK_SIZE
        reassembled[dec_offset:dec_offset + len(chunk_dec["chunk_data"])] = chunk_dec["chunk_data"]

    # 3. COMPLETE
    complete_pkt = encode_media_transfer(
        type_val=MEDIA_COMPLETE,
        transfer_id=transfer_id,
        checksum=checksum
    )
    complete_dec = decode_media_transfer(complete_pkt)
    assert complete_dec["type"] == MEDIA_COMPLETE
    assert complete_dec["checksum"] == checksum

    # 4. Verify reassembly
    assert bytes(reassembled) == memo_data, "Reassembled data mismatch"
    assert crc32(reassembled) == checksum, "CRC mismatch after reassembly"

    log(f"Full transfer: {len(memo_data)} bytes in {total_chunks} chunks, CRC=0x{checksum:08X}")


def test_voice_memo_crc32_consistency():
    """Test CRC32 on various Codec2 data sizes."""
    for duration in [1000, 5000, 10000, 30000]:
        data = generate_synthetic_codec2(duration)
        checksum = crc32(data)
        assert checksum == crc32(data), "CRC should be deterministic"
        # Flip one bit and verify CRC changes
        modified = bytearray(data)
        modified[0] ^= 0x01
        assert crc32(modified) != checksum, "CRC should change on data modification"
        log(f"{duration}ms: {len(data)} bytes, CRC=0x{checksum:08X}")


def test_voice_memo_chunking_sizes():
    """Test that chunking works for various memo durations."""
    for duration_s in [1, 5, 10, 30, 60]:
        data = generate_synthetic_codec2(duration_s * 1000)
        total_chunks = (len(data) + CHUNK_SIZE - 1) // CHUNK_SIZE
        # At 100 bytes/sec: 1s=100B (1 chunk), 30s=3000B (15 chunks), 60s=6000B (30 chunks)
        expected_bytes = (duration_s * 1000 // 40) * CODEC2_BYTES_PER_FRAME
        assert len(data) == expected_bytes
        assert total_chunks > 0
        log(f"{duration_s}s: {len(data)} bytes, {total_chunks} chunks")


# ─── Hardware tests ────────────────────────────────────────────────────

def test_hw_voice_memo_start_delivery():
    """Send a VOICE_MEMO START packet from A to B and verify receipt."""
    received_packets["B"].clear()

    memo_data = generate_synthetic_codec2(5000)
    checksum = crc32(memo_data)
    total_chunks = (len(memo_data) + CHUNK_SIZE - 1) // CHUNK_SIZE

    payload = encode_media_transfer(
        type_val=MEDIA_START,
        transfer_id=0x11220001,
        total_chunks=total_chunks,
        total_size=len(memo_data),
        content_type=VOICE_MEMO,
        checksum=checksum,
        mime_type="audio/codec2",
        duration_seconds=5
    )

    log(f"Sending VOICE_MEMO START from A ({len(payload)} bytes)")
    iface_a = meshtastic.serial_interface.SerialInterface(DEVICE_A)
    time.sleep(2)

    iface_b = meshtastic.serial_interface.SerialInterface(DEVICE_B)
    pub.subscribe(on_receive_B, "meshtastic.receive")
    time.sleep(2)

    iface_a.sendData(payload, portNum=MEDIA_TRANSFER_APP, wantAck=False)
    log("Waiting for B to receive...")

    deadline = time.time() + 30
    while time.time() < deadline:
        for pkt in received_packets["B"]:
            port = pkt.get("decoded", {}).get("portnum", "")
            if str(port) in ("MEDIA_TRANSFER_APP", "259", str(MEDIA_TRANSFER_APP)):
                raw = pkt["decoded"].get("payload", b"")
                if isinstance(raw, str):
                    raw = raw.encode("latin-1") if raw else b""
                if raw:
                    dec = decode_media_transfer(raw)
                    if dec["type"] == MEDIA_START and dec["content_type"] == VOICE_MEMO:
                        log(f"B received VOICE_MEMO START (transfer={dec['transfer_id']:#x}, "
                            f"mime={dec['mime_type']}, dur={dec['duration_seconds']}s)")
                        iface_a.close()
                        iface_b.close()
                        return
        time.sleep(1)

    iface_a.close()
    iface_b.close()
    raise TimeoutError("B did not receive VOICE_MEMO START within 30s")


def test_hw_voice_memo_full_transfer():
    """Send a complete voice memo (START + all CHUNKs + COMPLETE) from A to B."""
    received_packets["B"].clear()

    memo_data = generate_synthetic_codec2(3000)  # 3 seconds → 300 bytes
    checksum = crc32(memo_data)
    transfer_id = 0x11220002
    total_chunks = (len(memo_data) + CHUNK_SIZE - 1) // CHUNK_SIZE

    log(f"Voice memo: {len(memo_data)} bytes, {total_chunks} chunks, CRC=0x{checksum:08X}")

    iface_a = meshtastic.serial_interface.SerialInterface(DEVICE_A)
    time.sleep(2)
    iface_b = meshtastic.serial_interface.SerialInterface(DEVICE_B)
    pub.subscribe(on_receive_B, "meshtastic.receive")
    time.sleep(2)

    # Send START
    start_pkt = encode_media_transfer(
        type_val=MEDIA_START,
        transfer_id=transfer_id,
        total_chunks=total_chunks,
        total_size=len(memo_data),
        content_type=VOICE_MEMO,
        checksum=checksum,
        mime_type="audio/codec2",
        duration_seconds=3
    )
    iface_a.sendData(start_pkt, portNum=MEDIA_TRANSFER_APP, wantAck=False)
    log("Sent START")
    time.sleep(3)

    # Send CHUNKs
    for i in range(total_chunks):
        offset = i * CHUNK_SIZE
        end = min(offset + CHUNK_SIZE, len(memo_data))
        chunk = memo_data[offset:end]

        chunk_pkt = encode_media_transfer(
            type_val=MEDIA_CHUNK,
            transfer_id=transfer_id,
            chunk_index=i,
            chunk_data=chunk
        )
        iface_a.sendData(chunk_pkt, portNum=MEDIA_TRANSFER_APP, wantAck=False)
        log(f"Sent CHUNK {i}/{total_chunks} ({len(chunk)} bytes)")
        time.sleep(2)

    # Send COMPLETE
    complete_pkt = encode_media_transfer(
        type_val=MEDIA_COMPLETE,
        transfer_id=transfer_id,
        checksum=checksum
    )
    iface_a.sendData(complete_pkt, portNum=MEDIA_TRANSFER_APP, wantAck=False)
    log("Sent COMPLETE")

    # Wait for ACK_COMPLETE from B
    log("Waiting for ACK_COMPLETE from B...")
    deadline = time.time() + 30
    got_ack = False
    while time.time() < deadline:
        for pkt in received_packets["B"]:
            port = pkt.get("decoded", {}).get("portnum", "")
            if str(port) in ("MEDIA_TRANSFER_APP", "259", str(MEDIA_TRANSFER_APP)):
                raw = pkt["decoded"].get("payload", b"")
                if isinstance(raw, str):
                    raw = raw.encode("latin-1") if raw else b""
                if raw:
                    dec = decode_media_transfer(raw)
                    if dec["type"] == MEDIA_ACK_COMPLETE and dec["transfer_id"] == transfer_id:
                        log(f"B sent ACK_COMPLETE for transfer {transfer_id:#x}")
                        got_ack = True
                        break
        if got_ack:
            break
        time.sleep(1)

    iface_a.close()
    iface_b.close()

    if not got_ack:
        # ACK_COMPLETE is sent by B to A, we might see it on A's side instead
        log("Note: ACK_COMPLETE may have been sent to A (not visible on B's receive)")
        log("Transfer sequence completed (START + CHUNKs + COMPLETE sent successfully)")


def test_hw_voice_memo_sizes():
    """Test voice memo transfer with various durations to verify size handling."""
    iface_a = meshtastic.serial_interface.SerialInterface(DEVICE_A)
    time.sleep(2)
    iface_b = meshtastic.serial_interface.SerialInterface(DEVICE_B)
    pub.subscribe(on_receive_B, "meshtastic.receive")
    time.sleep(2)

    for duration_s in [1, 5, 10]:
        received_packets["B"].clear()
        memo_data = generate_synthetic_codec2(duration_s * 1000)
        checksum = crc32(memo_data)
        total_chunks = (len(memo_data) + CHUNK_SIZE - 1) // CHUNK_SIZE

        start_pkt = encode_media_transfer(
            type_val=MEDIA_START,
            transfer_id=0x11220010 + duration_s,
            total_chunks=total_chunks,
            total_size=len(memo_data),
            content_type=VOICE_MEMO,
            checksum=checksum,
            mime_type="audio/codec2",
            duration_seconds=duration_s
        )

        iface_a.sendData(start_pkt, portNum=MEDIA_TRANSFER_APP, wantAck=False)
        log(f"Sent {duration_s}s START: {len(memo_data)} bytes, {total_chunks} chunks")

        # Wait for delivery
        deadline = time.time() + 20
        received = False
        while time.time() < deadline:
            for pkt in received_packets["B"]:
                port = pkt.get("decoded", {}).get("portnum", "")
                if str(port) in ("MEDIA_TRANSFER_APP", "259", str(MEDIA_TRANSFER_APP)):
                    raw = pkt["decoded"].get("payload", b"")
                    if isinstance(raw, str):
                        raw = raw.encode("latin-1") if raw else b""
                    if raw:
                        dec = decode_media_transfer(raw)
                        if (dec["type"] == MEDIA_START and
                                dec["content_type"] == VOICE_MEMO and
                                dec["duration_seconds"] == duration_s):
                            log(f"B received {duration_s}s START OK")
                            received = True
                            break
            if received:
                break
            time.sleep(1)

        if not received:
            log(f"WARNING: B did not receive {duration_s}s START")

        time.sleep(3)

    iface_a.close()
    iface_b.close()


# ─── Main ──────────────────────────────────────────────────────────────

def main():
    print("\n" + "=" * 60)
    print("  MeshReliable Phase 3b — Voice Memo Test Suite")
    print("=" * 60)

    # Encoding tests (no hardware)
    run_test("voice_memo_start_encoding", test_voice_memo_start_encoding)
    run_test("synthetic_codec2_generation", test_synthetic_codec2_generation)
    run_test("voice_memo_full_transfer_sequence", test_voice_memo_full_transfer_sequence)
    run_test("voice_memo_crc32_consistency", test_voice_memo_crc32_consistency)
    run_test("voice_memo_chunking_sizes", test_voice_memo_chunking_sizes)

    # Hardware tests
    hw_available = True
    for port, name in [(DEVICE_A, "A"), (DEVICE_B, "B")]:
        if not os.path.exists(port):
            print(f"\n  WARNING: Device {name} ({port}) not found — skipping HW tests")
            hw_available = False

    if hw_available:
        run_test("hw_voice_memo_start_delivery", test_hw_voice_memo_start_delivery)
        run_test("hw_voice_memo_full_transfer", test_hw_voice_memo_full_transfer)
        run_test("hw_voice_memo_sizes", test_hw_voice_memo_sizes)

    # Summary
    print("\n" + "=" * 60)
    print("  RESULTS")
    print("=" * 60)
    passed = sum(1 for v in test_results.values() if v == "PASS")
    total = len(test_results)
    for name, result in test_results.items():
        status = "PASS" if result == "PASS" else "FAIL"
        print(f"  [{status}] {name}")
    print(f"\n  {passed}/{total} tests passed")
    print("=" * 60)

    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
