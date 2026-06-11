#pragma once

#include "ProtobufModule.h"
#include "concurrency/OSThread.h"
#include "mesh/generated/meshtastic/group_message.pb.h"

#include <unordered_map>
#include <vector>

// Tracks ACK status for a single outgoing group message
struct GroupAckTracker {
    uint32_t messageId;
    uint32_t groupId;
    uint32_t sendTime;           // millis() when originally sent
    uint32_t lastRebroadcast;    // millis() when last rebroadcast was done
    uint8_t rebroadcastCount;    // how many times we've rebroadcast
    uint8_t channel;             // which channel this was sent on
    std::vector<uint32_t> members;    // all expected member node IDs
    std::vector<uint32_t> ackedBy;    // node IDs that have ACKed
    meshtastic_GroupMessage originalMsg; // the original message for rebroadcast
};

class GroupMessageModule : private concurrency::OSThread, public ProtobufModule<meshtastic_GroupMessage>
{
  public:
    GroupMessageModule();

    // Send a text message to a group on the given channel
    void sendGroupText(uint8_t channelIndex, const char *text, const uint32_t *memberNodeIds, uint8_t memberCount);

    // Get the roster of known members for a group
    void requestRoster(uint8_t channelIndex, uint32_t groupId);

  protected:
    virtual int32_t runOnce() override;

    virtual bool handleReceivedProtobuf(const meshtastic_MeshPacket &mp, meshtastic_GroupMessage *decoded) override;

  private:
    // Maximum concurrent tracked messages
    static constexpr uint8_t MAX_TRACKED_MESSAGES = 8;

    // GRP-A: rebroadcast schedule — front-loaded, then a long tail; the LAST interval
    // repeats (hourly) until TRACKING_TIMEOUT, so un-ACKed members keep being retried
    // for up to ~24h. Only un-ACKed members are resent, so airtime stays low.
    static constexpr uint32_t REBROADCAST_INTERVALS[] = {15000,  30000,   60000,   120000,
                                                         300000, 600000,  1800000, 3600000};
    static constexpr uint8_t NUM_REBROADCAST_INTERVALS = 8;

    // How long to keep tracking/retrying a message before giving up (24 hours).
    static constexpr uint32_t TRACKING_TIMEOUT_MS = 86400000UL;

    // Next message ID counter
    uint32_t nextMessageId = 1;

    // Active ACK trackers for outgoing messages we're watching
    std::vector<GroupAckTracker> ackTrackers;

    // MQTT dedup: recently seen message IDs (to avoid MQTT echo loops)
    struct SeenMessageEntry {
        uint32_t messageId;
        uint32_t seenTime;
    };
    std::vector<SeenMessageEntry> recentlySeen;
    static constexpr uint32_t MQTT_DEDUP_WINDOW_MS = 60000; // 1 minute

    // Check and record a message for MQTT dedup
    bool isDuplicateMessage(uint32_t messageId);
    void cleanupSeenMessages();

    // Generate a unique message ID
    uint32_t generateMessageId();

    // Compute group ID from channel index
    uint32_t getGroupIdForChannel(uint8_t channelIndex);

    // Handle incoming message types
    void handleGroupText(const meshtastic_MeshPacket &mp, const meshtastic_GroupMessage &decoded);
    void handleGroupAck(const meshtastic_MeshPacket &mp, const meshtastic_GroupMessage &decoded);
    void handleGroupAllAcked(const meshtastic_MeshPacket &mp, const meshtastic_GroupMessage &decoded);
    void handleGroupJoin(const meshtastic_MeshPacket &mp, const meshtastic_GroupMessage &decoded);
    void handleGroupLeave(const meshtastic_MeshPacket &mp, const meshtastic_GroupMessage &decoded);
    void handleRosterRequest(const meshtastic_MeshPacket &mp, const meshtastic_GroupMessage &decoded);
    void handleRosterResponse(const meshtastic_MeshPacket &mp, const meshtastic_GroupMessage &decoded);

    // Send an ACK for a received group message
    void sendAck(uint8_t channelIndex, uint32_t messageId, uint32_t groupId, uint32_t toNode);

    // Send ALL_ACKED notification when all members have acknowledged
    void sendAllAcked(uint8_t channelIndex, uint32_t messageId, uint32_t groupId);

    // Rebroadcast a message that hasn't been fully ACKed yet
    void rebroadcastMessage(GroupAckTracker &tracker);

    // Check if a node ID is in the ACKed list
    bool hasAcked(const GroupAckTracker &tracker, uint32_t nodeId);

    // Send a GroupMessage protobuf as a channel broadcast
    void sendGroupPacket(uint8_t channelIndex, const meshtastic_GroupMessage &payload);

    // Send a GroupMessage protobuf as a RELIABLE UNICAST to one member (want_ack → routed
    // end-to-end ACK + persistent retry, the same mechanism that makes DMs ~100%). Used by
    // the retry path so un-ACKed / cross-band / offline members get DM-grade delivery.
    void sendGroupUnicast(uint8_t channelIndex, const meshtastic_GroupMessage &payload, uint32_t toNode);
};

extern GroupMessageModule *groupMessageModule;
