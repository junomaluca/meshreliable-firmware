#pragma once

#include "ProtobufModule.h"
#include "concurrency/OSThread.h"
#include "mesh/generated/meshtastic/cross_band.pb.h"

#include <unordered_map>
#include <vector>

// Band info tracked for each known node
struct NodeBandInfo {
    uint32_t nodeId;
    meshtastic_FrequencyBand primaryBand;
    bool isDualBand;
    std::vector<meshtastic_FrequencyBand> supportedBands;
    uint32_t lastSeenTime; // millis() when last updated
};

// Recently bridged message tracking (for dedup)
struct BridgedMessageEntry {
    uint32_t messageId;
    uint32_t timestamp; // millis() when bridged
};

class CrossBandModule : private concurrency::OSThread, public ProtobufModule<meshtastic_CrossBandMessage>
{
  public:
    CrossBandModule();

    // Get the band info for a specific node, or nullptr if unknown
    const NodeBandInfo *getNodeBandInfo(uint32_t nodeId) const;

    // Get our own primary band based on radio config
    meshtastic_FrequencyBand getOwnPrimaryBand() const;

    // Check if a node is on the same band as us
    bool isSameBand(uint32_t nodeId) const;

    // Check if a bridge path exists to a node (via dual-band device or MQTT)
    bool hasBridgePath(uint32_t nodeId) const;

    // Dual-band duty cycle: returns true if the next transmission should use 2.4 GHz
    bool shouldUse24GHz() const;

    // Get the sub-GHz duty cycle percentage (default 70)
    uint32_t getDutyCycleSubGhzPercent() const;

  protected:
    virtual int32_t runOnce() override;
    virtual bool handleReceivedProtobuf(const meshtastic_MeshPacket &mp, meshtastic_CrossBandMessage *decoded) override;

  private:
    static constexpr uint32_t BAND_ADVERT_INTERVAL_MS = 300000; // 5 minutes
    static constexpr uint32_t NODE_BAND_EXPIRE_MS = 3600000;    // 1 hour
    static constexpr uint32_t DEDUP_CLEANUP_INTERVAL_MS = 60000; // 1 minute

    uint32_t lastAdvertTime = 0;
    uint32_t lastDedupCleanup = 0;

    // Duty cycle tracking for dual-band time-division
    static constexpr uint32_t DUTY_CYCLE_WINDOW_MS = 10000; // 10-second rolling window
    uint32_t dutyCycleWindowStart = 0;
    uint32_t subGhzTxCount = 0;   // transmissions on sub-GHz in current window
    uint32_t ism24TxCount = 0;    // transmissions on 2.4 GHz in current window

    // Band info for all known nodes
    std::unordered_map<uint32_t, NodeBandInfo> nodeBands;

    // Dedup window for bridged messages
    std::vector<BridgedMessageEntry> dedupWindow;

    // Detect own frequency band from radio configuration
    meshtastic_FrequencyBand detectOwnBand() const;

    // Check if we are a dual-band device
    bool isOwnDeviceDualBand() const;

    // Send a band advertisement broadcast
    void sendBandAdvertisement();

    // Handle incoming band advertisement
    void handleBandAdvertisement(const meshtastic_MeshPacket &mp, const meshtastic_CrossBandMessage &decoded);

    // Handle a bridged message from another band
    void handleBridgedMessage(const meshtastic_MeshPacket &mp, const meshtastic_CrossBandMessage &decoded);

    // Handle band discovery request
    void handleBandDiscoveryRequest(const meshtastic_MeshPacket &mp, const meshtastic_CrossBandMessage &decoded);

    // Handle band discovery response
    void handleBandDiscoveryResponse(const meshtastic_MeshPacket &mp, const meshtastic_CrossBandMessage &decoded);

    // Check if a message ID was recently bridged (dedup)
    bool wasRecentlyBridged(uint32_t messageId) const;

    // Record a message as bridged
    void recordBridged(uint32_t messageId);

    // Clean up expired dedup entries
    void cleanupDedupWindow();

    // Clean up expired node band entries
    void cleanupExpiredNodes();

    // Get the dedup window duration in milliseconds
    uint32_t getDedupWindowMs() const;

    // Get the max bridge TTL from config
    uint32_t getMaxBridgeTTL() const;

    // Send a cross-band packet
    void sendCrossBandPacket(const meshtastic_CrossBandMessage &payload);

    // MQTT-based cross-band bridging
    void publishBridgedMessageToMqtt(const meshtastic_CrossBandMessage &msg);
    static const char *bandToString(meshtastic_FrequencyBand band);
};

extern CrossBandModule *crossBandModule;
