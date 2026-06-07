#pragma once

#include "mesh/MeshTypes.h"
#include "mesh/generated/meshtastic/mesh.pb.h"
#include <vector>

struct BufferedPacket {
    uint32_t time;
    uint32_t from;
    uint32_t to;
    uint32_t id;
    uint8_t channel;
    uint16_t portnum; // meshtastic_PortNum (must be uint16 — PRIVATE_APP = 256)
    uint8_t payload[256];
    uint16_t payload_size;
    int32_t rx_rssi;
    float rx_snr;
    uint8_t hop_start;
    uint8_t hop_limit;
};

class PhoneBufferModule
{
  public:
    static PhoneBufferModule *instance;

    PhoneBufferModule(uint16_t maxEntries = 200);

    /// Called from MeshService::sendToPhone() when phone is not connected
    void bufferPacket(const meshtastic_MeshPacket *p);

    /// Called from PhoneAPI::available() to drain buffered packets
    meshtastic_MeshPacket *getForPhone();

    /// How many packets are buffered (remaining to drain)
    uint16_t count() const;

    /// Reset read position so all buffered packets replay on next drain
    void resetReadIndex() { readIndex = 0; }

    /// Clear the buffer
    void clear();

  private:
    std::vector<BufferedPacket> buffer;
    uint16_t maxEntries;
    uint16_t readIndex = 0;

    bool shouldBuffer(uint16_t portnum) const;
    meshtastic_MeshPacket *reconstruct(const BufferedPacket &bp);
};

extern PhoneBufferModule *phoneBufferModule;
