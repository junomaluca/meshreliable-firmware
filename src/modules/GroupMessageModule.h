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

    // Rebroadcast schedule: intervals in ms (decaying)
    static constexpr uint32_t REBROADCAST_INTERVALS[] = {15000, 30000, 60000, 120000, 300000};
    static constexpr uint8_t MAX_REBROADCASTS = 5;

    // How long to keep tracking a message before giving up (10 minutes)
    static constexpr uint32_t TRACKING_TIMEOUT_MS = 600000;

    // Next message ID counter
    uint32_t nextMessageId = 1;

    // Active ACK trackers for outgoing messages we're watching
    std::vector<GroupAckTracker> ackTrackers;

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
    void sendAck(uint8_t channelIndex, uint32_t messageId, uint32_t groupId);

    // Send ALL_ACKED notification when all members have acknowledged
    void sendAllAcked(uint8_t channelIndex, uint32_t messageId, uint32_t groupId);

    // Rebroadcast a message that hasn't been fully ACKed yet
    void rebroadcastMessage(GroupAckTracker &tracker);

    // Check if a node ID is in the ACKed list
    bool hasAcked(const GroupAckTracker &tracker, uint32_t nodeId);

    // Send a GroupMessage protobuf on a channel
    void sendGroupPacket(uint8_t channelIndex, const meshtastic_GroupMessage &payload);
};

extern GroupMessageModule *groupMessageModule;
