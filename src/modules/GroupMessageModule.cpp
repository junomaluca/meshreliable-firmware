#include "GroupMessageModule.h"
#include <algorithm>
#include "MeshService.h"
#include "NodeDB.h"
#include "PowerStatus.h"
#include "Router.h"
#include "configuration.h"
#include "gps/RTC.h"
#include "mesh/Channels.h"

// Battery-aware retry throttling
static uint32_t getBatteryRetryMultiplier()
{
    if (!powerStatus || !powerStatus->getHasBattery())
        return 1;
    uint8_t pct = powerStatus->getBatteryChargePercent();
    if (pct == 0 || pct > 100)
        return 1;
    if (pct <= 5)
        return 0; // Disable rebroadcasts
    if (pct <= 10)
        return 4;
    if (pct <= 20)
        return 2;
    return 1;
}

GroupMessageModule *groupMessageModule = nullptr;

// Static constexpr definitions
constexpr uint32_t GroupMessageModule::REBROADCAST_INTERVALS[];

GroupMessageModule::GroupMessageModule()
    : concurrency::OSThread("GroupMessage"),
      ProtobufModule("GroupMessage", meshtastic_PortNum_GROUP_MESSAGE_APP, &meshtastic_GroupMessage_msg)
{
    // We want to see our own broadcasts for loopback testing
    loopbackOk = true;
}

int32_t GroupMessageModule::runOnce()
{
    // Check if module is enabled
    if (!moduleConfig.has_group_message || !moduleConfig.group_message.enabled) {
        return disable();
    }

    uint32_t now = millis();

    // Iterate through tracked messages and handle rebroadcasts/timeouts
    for (auto it = ackTrackers.begin(); it != ackTrackers.end();) {
        GroupAckTracker &tracker = *it;

        // Check if tracking has timed out
        if (now - tracker.sendTime > TRACKING_TIMEOUT_MS) {
            LOG_WARN("GroupMsg: message %u timed out, %d/%d ACKs received",
                     tracker.messageId, tracker.ackedBy.size(), tracker.members.size());
            it = ackTrackers.erase(it);
            continue;
        }

        // Check if it's time to rebroadcast (battery-throttled). GRP-A: keep retrying
        // un-ACKed members until TRACKING_TIMEOUT; after the explicit schedule, repeat
        // the last (long) interval. Jitter avoids synchronized collisions across nodes.
        uint32_t battMult = getBatteryRetryMultiplier();
        if (battMult > 0) {
            uint8_t idx = tracker.rebroadcastCount < NUM_REBROADCAST_INTERVALS
                              ? tracker.rebroadcastCount
                              : (uint8_t)(NUM_REBROADCAST_INTERVALS - 1);
            uint32_t interval = REBROADCAST_INTERVALS[idx] * battMult;
            int32_t jitter = (int32_t)random(interval / 4) - (int32_t)(interval / 8); // +/-12.5%
            uint32_t effInterval = (uint32_t)((int32_t)interval + jitter);
            if (now - tracker.lastRebroadcast >= effInterval) {
                rebroadcastMessage(tracker);
            }
        }

        ++it;
    }

    // Periodic cleanup of MQTT dedup entries
    cleanupSeenMessages();

    // Run every 5 seconds to check trackers
    return 5000;
}

bool GroupMessageModule::handleReceivedProtobuf(const meshtastic_MeshPacket &mp, meshtastic_GroupMessage *decoded)
{
    if (!decoded) {
        return false;
    }

    // Don't process our own messages (except for loopback testing)
    if (mp.from == nodeDB->getNodeNum() && decoded->type != meshtastic_GroupMessageType_GROUP_ALL_ACKED) {
        return false;
    }

    switch (decoded->type) {
    case meshtastic_GroupMessageType_GROUP_TEXT:
        handleGroupText(mp, *decoded);
        break;
    case meshtastic_GroupMessageType_GROUP_ACK:
        handleGroupAck(mp, *decoded);
        break;
    case meshtastic_GroupMessageType_GROUP_ALL_ACKED:
        handleGroupAllAcked(mp, *decoded);
        break;
    case meshtastic_GroupMessageType_GROUP_JOIN:
        handleGroupJoin(mp, *decoded);
        break;
    case meshtastic_GroupMessageType_GROUP_LEAVE:
        handleGroupLeave(mp, *decoded);
        break;
    case meshtastic_GroupMessageType_GROUP_ROSTER_REQUEST:
        handleRosterRequest(mp, *decoded);
        break;
    case meshtastic_GroupMessageType_GROUP_ROSTER_RESPONSE:
        handleRosterResponse(mp, *decoded);
        break;
    default:
        LOG_WARN("GroupMsg: unknown type %d", decoded->type);
        break;
    }

    // Return CONTINUE (false), NOT STOP (true): we've done our group-protocol side-effects
    // (ACK tracking, sending GROUP_ACK, etc.) above, but the packet must still be forwarded
    // to the phone/app — otherwise received group messages are silently consumed and never
    // appear in the app (and can't be measured). MediaTransferModule returns false for the
    // same reason. ProtobufModule maps false -> ProcessMessage::CONTINUE -> phone gets it.
    return false;
}

void GroupMessageModule::handleGroupText(const meshtastic_MeshPacket &mp, const meshtastic_GroupMessage &decoded)
{
    LOG_INFO("GroupMsg: TEXT from 0x%08x, msgId=%u, group=%u, text='%s'%s",
             mp.from, decoded.message_id, decoded.group_id, decoded.text,
             mp.via_mqtt ? " (via MQTT)" : "");

    // MQTT dedup: skip messages we've already processed (prevents echo loops)
    if (isDuplicateMessage(decoded.message_id)) {
        LOG_DEBUG("GroupMsg: dedup — already processed msgId=%u", decoded.message_id);
        return;
    }

    // Check if we're in the member list
    uint32_t ourNode = nodeDB->getNodeNum();
    bool isForUs = false;
    for (uint8_t i = 0; i < decoded.members_count; i++) {
        if (decoded.members[i] == ourNode) {
            isForUs = true;
            break;
        }
    }

    if (!isForUs && decoded.members_count > 0) {
        LOG_DEBUG("GroupMsg: TEXT not addressed to us, ignoring");
        return;
    }

    // Send ACK back to sender
    sendAck(mp.channel, decoded.message_id, decoded.group_id, mp.from);

    // The text message content is available in decoded.text for the UI/client to display
    LOG_INFO("GroupMsg: ACKed message %u from 0x%08x", decoded.message_id, mp.from);
}

void GroupMessageModule::handleGroupAck(const meshtastic_MeshPacket &mp, const meshtastic_GroupMessage &decoded)
{
    LOG_INFO("GroupMsg: ACK from 0x%08x for msgId=%u", mp.from, decoded.ack_message_id);

    // Find the tracker for this message
    for (auto &tracker : ackTrackers) {
        if (tracker.messageId == decoded.ack_message_id && tracker.groupId == decoded.group_id) {
            // Record the ACK if not already recorded
            if (!hasAcked(tracker, mp.from)) {
                tracker.ackedBy.push_back(mp.from);
                LOG_INFO("GroupMsg: %d/%d ACKs for msgId=%u",
                         tracker.ackedBy.size(), tracker.members.size(), tracker.messageId);

                // Check if all members have ACKed
                if (tracker.ackedBy.size() >= tracker.members.size()) {
                    LOG_INFO("GroupMsg: ALL members ACKed msgId=%u!", tracker.messageId);
                    sendAllAcked(tracker.channel, tracker.messageId, tracker.groupId);

                    // Remove the tracker
                    for (auto it = ackTrackers.begin(); it != ackTrackers.end(); ++it) {
                        if (it->messageId == tracker.messageId) {
                            ackTrackers.erase(it);
                            break;
                        }
                    }
                    return;
                }
            }
            return;
        }
    }

    LOG_DEBUG("GroupMsg: ACK for unknown msgId=%u (already completed or expired)", decoded.ack_message_id);
}

void GroupMessageModule::handleGroupAllAcked(const meshtastic_MeshPacket &mp, const meshtastic_GroupMessage &decoded)
{
    LOG_INFO("GroupMsg: ALL_ACKED from 0x%08x for msgId=%u", mp.from, decoded.message_id);
    // This is an informational broadcast — the sender is telling the group that everyone ACKed.
    // Clients/UI can use this to show a "delivered to all" indicator.
}

void GroupMessageModule::handleGroupJoin(const meshtastic_MeshPacket &mp, const meshtastic_GroupMessage &decoded)
{
    LOG_INFO("GroupMsg: JOIN from 0x%08x, node=0x%08x, group=%u",
             mp.from, decoded.member_node_id, decoded.group_id);
    // Group membership is managed by the sender's member list per message.
    // JOIN/LEAVE are informational announcements for clients.
}

void GroupMessageModule::handleGroupLeave(const meshtastic_MeshPacket &mp, const meshtastic_GroupMessage &decoded)
{
    LOG_INFO("GroupMsg: LEAVE from 0x%08x, node=0x%08x, group=%u",
             mp.from, decoded.member_node_id, decoded.group_id);
}

void GroupMessageModule::handleRosterRequest(const meshtastic_MeshPacket &mp, const meshtastic_GroupMessage &decoded)
{
    LOG_INFO("GroupMsg: ROSTER_REQUEST from 0x%08x for group=%u", mp.from, decoded.group_id);
    // For now, we don't maintain server-side rosters. The requester can get the roster
    // from the member lists in recent GROUP_TEXT messages.
}

void GroupMessageModule::handleRosterResponse(const meshtastic_MeshPacket &mp, const meshtastic_GroupMessage &decoded)
{
    LOG_INFO("GroupMsg: ROSTER_RESPONSE from 0x%08x for group=%u, %d members",
             mp.from, decoded.group_id, decoded.roster_count);
}

void GroupMessageModule::sendGroupText(uint8_t channelIndex, const char *text,
                                       const uint32_t *memberNodeIds, uint8_t memberCount)
{
    if (memberCount == 0 || memberCount > 32) {
        LOG_WARN("GroupMsg: invalid member count %d", memberCount);
        return;
    }

    uint32_t msgId = generateMessageId();
    uint32_t groupId = getGroupIdForChannel(channelIndex);

    meshtastic_GroupMessage msg = meshtastic_GroupMessage_init_zero;
    msg.type = meshtastic_GroupMessageType_GROUP_TEXT;
    msg.message_id = msgId;
    msg.group_id = groupId;
    strncpy(msg.text, text, sizeof(msg.text) - 1);
    msg.members_count = memberCount;
    for (uint8_t i = 0; i < memberCount; i++) {
        msg.members[i] = memberNodeIds[i];
    }
    msg.send_time = getTime();
    msg.rebroadcast_count = 0;

    // Deliver as N RELIABLE UNICASTS — one per member — i.e. literally "group = N DMs".
    // A broadcast loses ~30% per member on the first shot (vs ~0% for a DM), and the slower
    // rebroadcast retry doesn't always finish inside the measurement window. Sending each
    // member a want_ack unicast gives every member the routed end-to-end ACK + 24h persistent
    // retry that makes DMs ~100% — so group delivery converges to ~100% too. runOnce() still
    // re-unicasts any member that hasn't ACKed, as a backstop.
    uint32_t ourNode = nodeDB->getNodeNum();
    for (uint8_t i = 0; i < memberCount; i++) {
        if (memberNodeIds[i] == ourNode)
            continue;
        sendGroupUnicast(channelIndex, msg, memberNodeIds[i]);
    }

    // Set up ACK tracking
    if (ackTrackers.size() >= MAX_TRACKED_MESSAGES) {
        // Remove oldest tracker to make room
        LOG_WARN("GroupMsg: tracker limit reached, dropping oldest");
        ackTrackers.erase(ackTrackers.begin());
    }

    GroupAckTracker tracker;
    tracker.messageId = msgId;
    tracker.groupId = groupId;
    tracker.sendTime = millis();
    tracker.lastRebroadcast = millis();
    tracker.rebroadcastCount = 0;
    tracker.channel = channelIndex;
    tracker.originalMsg = msg;
    for (uint8_t i = 0; i < memberCount; i++) {
        tracker.members.push_back(memberNodeIds[i]);
    }
    ackTrackers.push_back(tracker);

    LOG_INFO("GroupMsg: sent TEXT msgId=%u to %d members on channel %d", msgId, memberCount, channelIndex);
}

void GroupMessageModule::sendAck(uint8_t channelIndex, uint32_t messageId, uint32_t groupId, uint32_t toNode)
{
    meshtastic_GroupMessage ack = meshtastic_GroupMessage_init_zero;
    ack.type = meshtastic_GroupMessageType_GROUP_ACK;
    ack.ack_message_id = messageId;
    ack.group_id = groupId;
    ack.member_node_id = nodeDB->getNodeNum();

    // Send the ACK as a RELIABLE UNICAST back to the original sender (want_ack -> routed
    // end-to-end ACK + retry), NOT a fire-and-forget broadcast. A broadcast ACK is easily
    // lost, so the sender can't reliably detect "all members ACKed" even when delivery
    // succeeded — which capped group success at ~p^N. A reliable ACK makes the round trip
    // as dependable as a DM, so group delivery converges to ~100%.
    if (toNode != 0 && toNode != NODENUM_BROADCAST)
        sendGroupUnicast(channelIndex, ack, toNode);
    else
        sendGroupPacket(channelIndex, ack);
    LOG_DEBUG("GroupMsg: sent ACK for msgId=%u to 0x%08x", messageId, toNode);
}

void GroupMessageModule::sendAllAcked(uint8_t channelIndex, uint32_t messageId, uint32_t groupId)
{
    meshtastic_GroupMessage allAcked = meshtastic_GroupMessage_init_zero;
    allAcked.type = meshtastic_GroupMessageType_GROUP_ALL_ACKED;
    allAcked.message_id = messageId;
    allAcked.group_id = groupId;

    sendGroupPacket(channelIndex, allAcked);
    LOG_INFO("GroupMsg: sent ALL_ACKED for msgId=%u", messageId);
}

void GroupMessageModule::rebroadcastMessage(GroupAckTracker &tracker)
{
    tracker.rebroadcastCount++;
    tracker.lastRebroadcast = millis();
    tracker.originalMsg.rebroadcast_count = tracker.rebroadcastCount;

    // HYBRID delivery: the FIRST attempt is a broadcast (one transmission reaches every
    // in-range, same-band member at once). The RETRY, however, is a RELIABLE UNICAST to
    // each member that hasn't ACKed yet — not another broadcast. A broadcast is
    // fire-and-forget (no link-layer ACK) and never crosses bands; a want_ack unicast gets
    // the routed end-to-end ACK + 24h persistent retry (the proven DM mechanism), so an
    // un-ACKed member — including cross-band (MQTT-routed) or temporarily-offline ones — is
    // delivered to reliably and independently, without one straggler blocking the rest.
    uint32_t ourNode = nodeDB->getNodeNum();
    uint8_t sent = 0;
    for (uint32_t member : tracker.members) {
        if (member == ourNode)
            continue; // never unicast to ourselves
        bool acked = false;
        for (uint32_t a : tracker.ackedBy) {
            if (a == member) {
                acked = true;
                break;
            }
        }
        if (acked)
            continue;
        sendGroupUnicast(tracker.channel, tracker.originalMsg, member);
        sent++;
    }
    LOG_INFO("GroupMsg: reliable-unicast retry #%d for msgId=%u to %d un-ACKed member(s) (%d/%d ACKed)",
             tracker.rebroadcastCount, tracker.messageId, sent, (int)tracker.ackedBy.size(),
             (int)tracker.members.size());
}

bool GroupMessageModule::hasAcked(const GroupAckTracker &tracker, uint32_t nodeId)
{
    for (auto id : tracker.ackedBy) {
        if (id == nodeId) {
            return true;
        }
    }
    return false;
}

void GroupMessageModule::sendGroupPacket(uint8_t channelIndex, const meshtastic_GroupMessage &payload)
{
    meshtastic_MeshPacket *p = allocDataProtobuf(payload);
    p->to = NODENUM_BROADCAST;
    p->channel = channelIndex;
    p->want_ack = false; // We handle ACKs at the group message layer, not the mesh layer
    p->decoded.want_response = false;
    p->priority = meshtastic_MeshPacket_Priority_DEFAULT;

    service->sendToMesh(p);
}

void GroupMessageModule::sendGroupUnicast(uint8_t channelIndex, const meshtastic_GroupMessage &payload, uint32_t toNode)
{
    meshtastic_MeshPacket *p = allocDataProtobuf(payload);
    p->to = toNode;
    p->channel = channelIndex;
    // Reliable unicast: the router/NextHopRouter gives this an end-to-end ACK and the
    // MeshReliable persistent retry (24h) — i.e. the same machinery that makes DMs ~100%.
    // The recipient still replies with a GROUP_ACK, which marks it ACKed in the tracker.
    p->want_ack = true;
    p->decoded.want_response = false;
    p->priority = meshtastic_MeshPacket_Priority_RELIABLE;

    service->sendToMesh(p);
}

uint32_t GroupMessageModule::generateMessageId()
{
    // Combine our node ID with an incrementing counter for uniqueness
    return (nodeDB->getNodeNum() & 0xFFFF0000) | (nextMessageId++ & 0x0000FFFF);
}

uint32_t GroupMessageModule::getGroupIdForChannel(uint8_t channelIndex)
{
    // Use the channel hash as the group ID (cast from int16_t to uint32_t)
    return (uint32_t)(uint16_t)channels.getHash(channelIndex);
}

void GroupMessageModule::requestRoster(uint8_t channelIndex, uint32_t groupId)
{
    meshtastic_GroupMessage req = meshtastic_GroupMessage_init_zero;
    req.type = meshtastic_GroupMessageType_GROUP_ROSTER_REQUEST;
    req.group_id = groupId;

    sendGroupPacket(channelIndex, req);
    LOG_INFO("GroupMsg: sent ROSTER_REQUEST for group=%u", groupId);
}

bool GroupMessageModule::isDuplicateMessage(uint32_t messageId)
{
    for (auto &entry : recentlySeen) {
        if (entry.messageId == messageId) {
            return true;
        }
    }
    // Not seen — record it
    SeenMessageEntry entry;
    entry.messageId = messageId;
    entry.seenTime = millis();
    recentlySeen.push_back(entry);
    return false;
}

void GroupMessageModule::cleanupSeenMessages()
{
    uint32_t now = millis();
    recentlySeen.erase(
        std::remove_if(recentlySeen.begin(), recentlySeen.end(),
                        [now](const SeenMessageEntry &e) {
                            return (now - e.seenTime) > MQTT_DEDUP_WINDOW_MS;
                        }),
        recentlySeen.end());
}
