#include "PhoneBufferModule.h"
#include "MeshService.h"
#include "RTC.h"
#include "mesh/generated/meshtastic/portnums.pb.h"

PhoneBufferModule *phoneBufferModule = nullptr;
PhoneBufferModule *PhoneBufferModule::instance = nullptr;

PhoneBufferModule::PhoneBufferModule(uint16_t maxEntries) : maxEntries(maxEntries)
{
    instance = this;
    // Don't pre-reserve — operator new uses internal SRAM (not PSRAM) on ESP32,
    // so a 140KB reserve (500 × 280 bytes) causes std::bad_alloc on boot.
    // The vector grows on demand; individual push_back allocations are small enough
    // to succeed from internal SRAM or get promoted to PSRAM as the buffer grows.
}

bool PhoneBufferModule::shouldBuffer(uint16_t portnum) const
{
    // TEXT_MESSAGE_APP: user text messages
    // PRIVATE_APP: voice memo/image data forwarded to phone via forwardToPhone()
    // Note: MEDIA_TRANSFER_APP excluded — those are protocol control packets (START/CHUNK/ACK/NACK),
    // not user content. Buffering them causes duplicate entries in the app.
    return portnum == meshtastic_PortNum_TEXT_MESSAGE_APP ||
           portnum == meshtastic_PortNum_PRIVATE_APP;
}

void PhoneBufferModule::bufferPacket(const meshtastic_MeshPacket *p)
{
    if (!p || !p->decoded.payload.size)
        return;

    uint16_t port = (uint16_t)p->decoded.portnum;
    if (!shouldBuffer(port))
        return;

    // If buffer is full, drop the oldest entry
    if (buffer.size() >= maxEntries) {
        LOG_WARN("PhoneBuffer: full (%u), dropping oldest", maxEntries);
        buffer.erase(buffer.begin());
        if (readIndex > 0)
            readIndex--;
    }

    BufferedPacket bp = {};
    bp.time = p->rx_time ? p->rx_time : getValidTime(RTCQualityNTP, false);
    bp.from = p->from;
    bp.to = p->to;
    bp.id = p->id;
    bp.channel = p->channel;
    bp.portnum = port;
    bp.rx_rssi = p->rx_rssi;
    bp.rx_snr = p->rx_snr;
    bp.hop_start = p->hop_start;
    bp.hop_limit = p->hop_limit;

    uint16_t copyLen = p->decoded.payload.size;
    if (copyLen > sizeof(bp.payload))
        copyLen = sizeof(bp.payload);
    memcpy(bp.payload, p->decoded.payload.bytes, copyLen);
    bp.payload_size = copyLen;

    buffer.push_back(bp);
    LOG_INFO("PhoneBuffer: buffered packet id=0x%08x port=%u from=0x%04x (%u total)", p->id, port, p->from,
             (unsigned)buffer.size());
}

meshtastic_MeshPacket *PhoneBufferModule::getForPhone()
{
    if (readIndex >= buffer.size()) {
        // All drained — clear
        if (!buffer.empty()) {
            LOG_INFO("PhoneBuffer: drain complete, clearing %u entries", (unsigned)buffer.size());
            clear();
        }
        return nullptr;
    }

    if (readIndex == 0 && !buffer.empty()) {
        LOG_INFO("PhoneBuffer: draining %u buffered packets to phone", (unsigned)buffer.size());
    }

    meshtastic_MeshPacket *p = reconstruct(buffer[readIndex]);
    readIndex++;
    return p;
}

meshtastic_MeshPacket *PhoneBufferModule::reconstruct(const BufferedPacket &bp)
{
    meshtastic_MeshPacket *p = packetPool.allocZeroed();
    if (!p) {
        LOG_WARN("PhoneBuffer: packetPool exhausted, can't reconstruct");
        return nullptr;
    }

    p->from = bp.from;
    p->to = bp.to;
    p->id = bp.id;
    p->channel = bp.channel;
    p->rx_time = bp.time;
    p->rx_rssi = bp.rx_rssi;
    p->rx_snr = bp.rx_snr;
    p->hop_start = bp.hop_start;
    p->hop_limit = bp.hop_limit;
    p->which_payload_variant = meshtastic_MeshPacket_decoded_tag;
    p->decoded.portnum = (meshtastic_PortNum)bp.portnum;
    p->decoded.payload.size = bp.payload_size;
    memcpy(p->decoded.payload.bytes, bp.payload, bp.payload_size);

    return p;
}

uint16_t PhoneBufferModule::count() const
{
    if (readIndex >= buffer.size())
        return 0;
    return (uint16_t)(buffer.size() - readIndex);
}

void PhoneBufferModule::clear()
{
    buffer.clear();
    readIndex = 0;
}
