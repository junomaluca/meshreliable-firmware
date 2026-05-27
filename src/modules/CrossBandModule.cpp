#include "CrossBandModule.h"
#include "MeshService.h"
#include "NodeDB.h"
#include "Router.h"
#include "configuration.h"

CrossBandModule *crossBandModule = nullptr;

CrossBandModule::CrossBandModule()
    : concurrency::OSThread("CrossBand"),
      ProtobufModule("CrossBand", meshtastic_PortNum_CROSS_BAND_APP, &meshtastic_CrossBandMessage_msg)
{
}

meshtastic_FrequencyBand CrossBandModule::detectOwnBand() const
{
    // Determine our primary band from the radio region config
    switch (config.lora.region) {
    case meshtastic_Config_LoRaConfig_RegionCode_US:
        return meshtastic_FrequencyBand_BAND_US_915;
    case meshtastic_Config_LoRaConfig_RegionCode_EU_868:
        return meshtastic_FrequencyBand_BAND_EU_868;
    case meshtastic_Config_LoRaConfig_RegionCode_CN:
        return meshtastic_FrequencyBand_BAND_CN_470;
    case meshtastic_Config_LoRaConfig_RegionCode_JP:
    case meshtastic_Config_LoRaConfig_RegionCode_KR:
        return meshtastic_FrequencyBand_BAND_JP_920;
    case meshtastic_Config_LoRaConfig_RegionCode_IN:
        return meshtastic_FrequencyBand_BAND_IN_865;
    case meshtastic_Config_LoRaConfig_RegionCode_ANZ:
        return meshtastic_FrequencyBand_BAND_ANZ_915;
    case meshtastic_Config_LoRaConfig_RegionCode_EU_433:
        return meshtastic_FrequencyBand_BAND_EU_433;
    default:
        return meshtastic_FrequencyBand_BAND_UNKNOWN;
    }
}

bool CrossBandModule::isOwnDeviceDualBand() const
{
    // Check if this device has dual-band radio hardware (LR1121 or SX1280 + SX126x)
#if defined(USE_LR1121)
    return true;
#elif defined(USE_SX1280) && (defined(USE_SX1262) || defined(USE_SX1276))
    return true;
#else
    return false;
#endif
}

meshtastic_FrequencyBand CrossBandModule::getOwnPrimaryBand() const
{
    return detectOwnBand();
}

bool CrossBandModule::isSameBand(uint32_t nodeId) const
{
    auto it = nodeBands.find(nodeId);
    if (it == nodeBands.end()) {
        return true; // assume same band if unknown
    }
    return it->second.primaryBand == detectOwnBand();
}

bool CrossBandModule::hasBridgePath(uint32_t nodeId) const
{
    auto it = nodeBands.find(nodeId);
    if (it == nodeBands.end()) {
        return false; // unknown node, no known bridge path
    }

    if (it->second.primaryBand == detectOwnBand()) {
        return true; // same band, direct path
    }

    // Check if any dual-band node exists that bridges both bands
    meshtastic_FrequencyBand ownBand = detectOwnBand();
    meshtastic_FrequencyBand targetBand = it->second.primaryBand;

    for (auto &pair : nodeBands) {
        if (pair.second.isDualBand) {
            bool hasOurBand = false;
            bool hasTargetBand = false;
            for (auto band : pair.second.supportedBands) {
                if (band == ownBand) hasOurBand = true;
                if (band == targetBand) hasTargetBand = true;
            }
            if (hasOurBand && hasTargetBand) {
                return true;
            }
        }
    }

    return false;
}

const NodeBandInfo *CrossBandModule::getNodeBandInfo(uint32_t nodeId) const
{
    auto it = nodeBands.find(nodeId);
    if (it != nodeBands.end()) {
        return &it->second;
    }
    return nullptr;
}

uint32_t CrossBandModule::getDedupWindowMs() const
{
    uint32_t minutes = 60; // default
    if (moduleConfig.has_cross_band && moduleConfig.cross_band.dedup_window_minutes > 0) {
        minutes = moduleConfig.cross_band.dedup_window_minutes;
        if (minutes < 10) minutes = 10;
        if (minutes > 360) minutes = 360;
    }
    return minutes * 60000UL;
}

uint32_t CrossBandModule::getMaxBridgeTTL() const
{
    uint32_t ttl = 3; // default
    if (moduleConfig.has_cross_band && moduleConfig.cross_band.max_bridge_ttl > 0) {
        ttl = moduleConfig.cross_band.max_bridge_ttl;
        if (ttl > 5) ttl = 5;
    }
    return ttl;
}

int32_t CrossBandModule::runOnce()
{
    if (!moduleConfig.has_cross_band || !moduleConfig.cross_band.enabled) {
        return disable();
    }

    uint32_t now = millis();

    // Periodic band advertisement
    if (now - lastAdvertTime >= BAND_ADVERT_INTERVAL_MS) {
        sendBandAdvertisement();
        lastAdvertTime = now;
    }

    // Periodic dedup window cleanup
    if (now - lastDedupCleanup >= DEDUP_CLEANUP_INTERVAL_MS) {
        cleanupDedupWindow();
        cleanupExpiredNodes();
        lastDedupCleanup = now;
    }

    return 10000; // check every 10 seconds
}

bool CrossBandModule::handleReceivedProtobuf(const meshtastic_MeshPacket &mp, meshtastic_CrossBandMessage *decoded)
{
    if (!decoded) return false;

    switch (decoded->type) {
    case meshtastic_CrossBandMessageType_BAND_ADVERTISEMENT:
        handleBandAdvertisement(mp, *decoded);
        break;
    case meshtastic_CrossBandMessageType_BRIDGED_MESSAGE:
        handleBridgedMessage(mp, *decoded);
        break;
    case meshtastic_CrossBandMessageType_BAND_DISCOVERY_REQUEST:
        handleBandDiscoveryRequest(mp, *decoded);
        break;
    case meshtastic_CrossBandMessageType_BAND_DISCOVERY_RESPONSE:
        handleBandDiscoveryResponse(mp, *decoded);
        break;
    default:
        LOG_WARN("CrossBand: unknown type %d", decoded->type);
        break;
    }

    return true;
}

void CrossBandModule::sendBandAdvertisement()
{
    meshtastic_CrossBandMessage pkt = meshtastic_CrossBandMessage_init_zero;
    pkt.type = meshtastic_CrossBandMessageType_BAND_ADVERTISEMENT;
    pkt.node_id = nodeDB->getNodeNum();
    pkt.primary_band = detectOwnBand();
    pkt.is_dual_band = isOwnDeviceDualBand();

    // Populate supported bands
    pkt.supported_bands_count = 0;
    meshtastic_FrequencyBand primary = detectOwnBand();
    if (primary != meshtastic_FrequencyBand_BAND_UNKNOWN) {
        pkt.supported_bands[pkt.supported_bands_count++] = primary;
    }

    // If dual-band, add 2.4 GHz
    if (isOwnDeviceDualBand()) {
        pkt.supported_bands[pkt.supported_bands_count++] = meshtastic_FrequencyBand_BAND_ISM_2400;
    }

    sendCrossBandPacket(pkt);
    LOG_DEBUG("CrossBand: sent band advertisement (band=%d, dual=%d)", primary, pkt.is_dual_band);
}

void CrossBandModule::handleBandAdvertisement(const meshtastic_MeshPacket &mp,
                                               const meshtastic_CrossBandMessage &decoded)
{
    uint32_t nodeId = decoded.node_id;
    if (nodeId == 0) nodeId = mp.from;

    NodeBandInfo info;
    info.nodeId = nodeId;
    info.primaryBand = decoded.primary_band;
    info.isDualBand = decoded.is_dual_band;
    info.supportedBands.clear();
    for (uint32_t i = 0; i < decoded.supported_bands_count; i++) {
        info.supportedBands.push_back(decoded.supported_bands[i]);
    }
    info.lastSeenTime = millis();

    nodeBands[nodeId] = info;

    LOG_INFO("CrossBand: node 0x%08x advertises band=%d, dual=%d, %d bands",
             nodeId, decoded.primary_band, decoded.is_dual_band, decoded.supported_bands_count);
}

void CrossBandModule::handleBridgedMessage(const meshtastic_MeshPacket &mp,
                                            const meshtastic_CrossBandMessage &decoded)
{
    // Anti-loop: check if this message was already bridged
    if (wasRecentlyBridged(decoded.original_message_id)) {
        LOG_DEBUG("CrossBand: dedup — already bridged msg %u", decoded.original_message_id);
        return;
    }

    // Anti-loop: check bridge TTL
    if (decoded.bridge_ttl == 0) {
        LOG_DEBUG("CrossBand: bridge TTL expired for msg %u", decoded.original_message_id);
        return;
    }

    // Anti-loop: don't re-bridge back to the source band
    meshtastic_FrequencyBand ownBand = detectOwnBand();
    if (decoded.source_band == ownBand) {
        LOG_DEBUG("CrossBand: skipping re-bridge to source band for msg %u", decoded.original_message_id);
        return;
    }

    // Record this message as bridged
    recordBridged(decoded.original_message_id);

    LOG_INFO("CrossBand: received bridged msg %u from band %d (TTL=%u, %u bytes payload)",
             decoded.original_message_id, decoded.source_band, decoded.bridge_ttl,
             decoded.bridged_payload_size);

    // If we're a dual-band device, we could re-bridge onto our other band
    // For now, just inject the bridged payload into the local mesh
    if (decoded.bridged_payload_size > 0 && decoded.original_portnum > 0) {
        // The bridged payload contains the raw protobuf of the original message
        // Re-inject it into the local mesh with the original portnum
        meshtastic_MeshPacket *p = allocDataPacket();
        if (p) {
            p->to = decoded.original_dest;
            p->channel = decoded.original_channel;
            p->decoded.portnum = (meshtastic_PortNum)decoded.original_portnum;
            p->decoded.payload.size = decoded.bridged_payload_size;
            memcpy(p->decoded.payload.bytes, decoded.bridged_payload, decoded.bridged_payload_size);
            p->want_ack = false;
            p->priority = meshtastic_MeshPacket_Priority_DEFAULT;
            service->sendToMesh(p);
            LOG_INFO("CrossBand: re-injected bridged msg %u onto local mesh (portnum=%d)",
                     decoded.original_message_id, decoded.original_portnum);
        }
    }
}

void CrossBandModule::handleBandDiscoveryRequest(const meshtastic_MeshPacket &mp,
                                                   const meshtastic_CrossBandMessage &decoded)
{
    LOG_DEBUG("CrossBand: discovery request from 0x%08x", mp.from);

    // Respond with our band info
    meshtastic_CrossBandMessage response = meshtastic_CrossBandMessage_init_zero;
    response.type = meshtastic_CrossBandMessageType_BAND_DISCOVERY_RESPONSE;
    response.node_id = nodeDB->getNodeNum();
    response.primary_band = detectOwnBand();
    response.is_dual_band = isOwnDeviceDualBand();

    response.supported_bands_count = 0;
    meshtastic_FrequencyBand primary = detectOwnBand();
    if (primary != meshtastic_FrequencyBand_BAND_UNKNOWN) {
        response.supported_bands[response.supported_bands_count++] = primary;
    }
    if (isOwnDeviceDualBand()) {
        response.supported_bands[response.supported_bands_count++] = meshtastic_FrequencyBand_BAND_ISM_2400;
    }

    sendCrossBandPacket(response);
}

void CrossBandModule::handleBandDiscoveryResponse(const meshtastic_MeshPacket &mp,
                                                    const meshtastic_CrossBandMessage &decoded)
{
    // Same as band advertisement — update our node band map
    handleBandAdvertisement(mp, decoded);
}

bool CrossBandModule::wasRecentlyBridged(uint32_t messageId) const
{
    for (auto &entry : dedupWindow) {
        if (entry.messageId == messageId) {
            return true;
        }
    }
    return false;
}

void CrossBandModule::recordBridged(uint32_t messageId)
{
    BridgedMessageEntry entry;
    entry.messageId = messageId;
    entry.timestamp = millis();
    dedupWindow.push_back(entry);
}

void CrossBandModule::cleanupDedupWindow()
{
    uint32_t now = millis();
    uint32_t windowMs = getDedupWindowMs();

    dedupWindow.erase(
        std::remove_if(dedupWindow.begin(), dedupWindow.end(),
                        [now, windowMs](const BridgedMessageEntry &e) {
                            return (now - e.timestamp) > windowMs;
                        }),
        dedupWindow.end());
}

void CrossBandModule::cleanupExpiredNodes()
{
    uint32_t now = millis();

    for (auto it = nodeBands.begin(); it != nodeBands.end();) {
        if ((now - it->second.lastSeenTime) > NODE_BAND_EXPIRE_MS) {
            LOG_DEBUG("CrossBand: expiring band info for node 0x%08x", it->first);
            it = nodeBands.erase(it);
        } else {
            ++it;
        }
    }
}

void CrossBandModule::sendCrossBandPacket(const meshtastic_CrossBandMessage &payload)
{
    meshtastic_MeshPacket *p = allocDataProtobuf(payload);
    p->to = NODENUM_BROADCAST;
    p->want_ack = false;
    p->decoded.want_response = false;
    p->priority = meshtastic_MeshPacket_Priority_DEFAULT;
    service->sendToMesh(p);
}
