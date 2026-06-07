"""Meshtastic packet decryption for MQTT monitoring.

Decrypts encrypted MeshPackets using channel PSKs, matching the firmware's
CryptoEngine (AES-CTR) and Channels::getKey() PSK expansion.
"""

import struct
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

# ─── Channel PSK Constants ─────────────────────────────────────────────

# Maluca channel (primary) — 32-byte AES-256 key from userPrefs.jsonc
MALUCA_PSK = bytes([
    0x42, 0x73, 0x30, 0x66, 0x76, 0x5a, 0x52, 0x52,
    0x64, 0x42, 0x68, 0x67, 0x49, 0x38, 0x37, 0x64,
    0x69, 0x67, 0x72, 0x48, 0x41, 0x63, 0x33, 0x32,
    0x53, 0x71, 0x55, 0x39, 0x7a, 0x51, 0x72, 0x6e,
])

# Default PSK base (16-byte AES-128) — firmware's defaultpsk[]
_DEFAULT_PSK_BASE = bytes([
    0xd4, 0xf1, 0xbb, 0x3a, 0x20, 0x29, 0x07, 0x59,
    0xf0, 0xbc, 0xff, 0xab, 0xcf, 0x4e, 0x69, 0x01,
])

# LongFast channel uses PSK index 1 = default PSK unmodified
DEFAULT_PSK = _DEFAULT_PSK_BASE

# Channel name -> PSK mapping
CHANNEL_PSKS = {
    'maluca': MALUCA_PSK,
    'LongFast': DEFAULT_PSK,
}


def expand_psk(raw_psk: bytes) -> bytes:
    """Expand short PSK to full key, matching firmware Channels::getKey().

    - 0 bytes or empty: no encryption
    - 1 byte: expand using defaultpsk base + index offset
    - <16 bytes: pad to 16 (AES-128)
    - 16 bytes: AES-128
    - 17-31 bytes: pad to 32 (AES-256)
    - 32 bytes: AES-256
    """
    if not raw_psk:
        return b''

    if len(raw_psk) == 1:
        index = raw_psk[0]
        if index == 0:
            return b''
        key = bytearray(_DEFAULT_PSK_BASE)
        key[-1] = (key[-1] + index - 1) & 0xFF
        return bytes(key)

    if len(raw_psk) < 16:
        return raw_psk.ljust(16, b'\x00')
    if 16 < len(raw_psk) < 32:
        return raw_psk.ljust(32, b'\x00')
    return raw_psk


def build_nonce(packet_id: int, from_node: int) -> bytes:
    """Build 16-byte AES-CTR nonce matching firmware CryptoEngine::initNonce().

    Layout (all little-endian):
      [0:8]   packet_id as uint64
      [8:12]  from_node as uint32
      [12:16] 0x00000000 (counter start)
    """
    return struct.pack('<QI', packet_id, from_node) + b'\x00\x00\x00\x00'


def decrypt_packet(encrypted: bytes, key: bytes, packet_id: int, from_node: int) -> bytes:
    """Decrypt an encrypted MeshPacket payload using AES-CTR."""
    if not key or not encrypted:
        return b''
    nonce = build_nonce(packet_id, from_node)
    cipher = Cipher(algorithms.AES(key), modes.CTR(nonce))
    decryptor = cipher.decryptor()
    return decryptor.update(encrypted) + decryptor.finalize()


def decrypt_mesh_packet(packet, channel_id: str = None):
    """Decrypt a mesh_pb2.MeshPacket's encrypted field.

    Args:
        packet: A mesh_pb2.MeshPacket with .encrypted field set
        channel_id: Channel name (e.g. 'maluca', 'LongFast') to select PSK.
                    If None, tries all known PSKs.

    Returns:
        A mesh_pb2.Data protobuf on success, None on failure.
    """
    from meshtastic.protobuf import mesh_pb2

    if not packet.HasField('encrypted') or not packet.encrypted:
        return None

    from_id = packet.from_
    packet_id = packet.id

    # Determine which PSKs to try
    if channel_id and channel_id in CHANNEL_PSKS:
        psks_to_try = [(channel_id, CHANNEL_PSKS[channel_id])]
    else:
        psks_to_try = list(CHANNEL_PSKS.items())

    for name, psk in psks_to_try:
        try:
            decrypted = decrypt_packet(packet.encrypted, psk, packet_id, from_id)
            # Try parsing as Data protobuf
            data = mesh_pb2.Data()
            data.ParseFromString(decrypted)
            # Sanity check: portnum should be in valid range (1-256)
            if 1 <= data.portnum <= 256:
                return data
        except Exception:
            continue

    return None
