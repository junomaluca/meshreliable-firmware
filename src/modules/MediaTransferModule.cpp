#include "MediaTransferModule.h"
#include "MeshService.h"
#include "NodeDB.h"
#include "PowerStatus.h"
#include "Router.h"
#include "configuration.h"
#include "gps/RTC.h"

// Battery-aware retry throttling: returns multiplier for retry intervals
// At full charge: 1x. Below 20%: 2x. Below 10%: 4x. Below 5%: 0 (disable retries).
static uint32_t getBatteryRetryMultiplier()
{
    if (!powerStatus || !powerStatus->getHasBattery())
        return 1; // No battery info, run at full speed
    uint8_t pct = powerStatus->getBatteryChargePercent();
    if (pct == 0 || pct > 100)
        return 1; // Unknown or plugged in
    if (pct <= 5)
        return 0; // Disable retries
    if (pct <= 10)
        return 4;
    if (pct <= 20)
        return 2;
    return 1;
}

MediaTransferModule *mediaTransferModule = nullptr;

MediaTransferModule::MediaTransferModule()
    : concurrency::OSThread("MediaTransfer"),
      ProtobufModule("MediaTransfer", meshtastic_PortNum_MEDIA_TRANSFER_APP, &meshtastic_MediaTransfer_msg)
{
}

uint32_t MediaTransferModule::getChunkSize()
{
    uint32_t size = 200; // default
    if (moduleConfig.has_media_transfer && moduleConfig.media_transfer.chunk_size_bytes > 0) {
        size = moduleConfig.media_transfer.chunk_size_bytes;
        if (size < 100) size = 100;
        if (size > 237) size = 237;
    }
    return size;
}

uint32_t MediaTransferModule::generateTransferId()
{
    return (nodeDB->getNodeNum() & 0xFFFF0000) | (nextTransferId++ & 0x0000FFFF);
}

int32_t MediaTransferModule::runOnce()
{
    // Process deferred ACK_COMPLETE / NACK from handleReceivedProtobuf FIRST,
    // before the disable check. These are deferred because the handleReceived
    // call chain is too deep (decrypt → callModules → protobuf decode → handler)
    // and adding protobuf encode + AES encrypt + queue operations overflows
    // the loopTask stack.
    if (pendingResponse.type == PendingResponse::ACK) {
        LOG_INFO("MediaXfer: processing deferred ACK_COMPLETE tid=%u to=0x%08x",
                 pendingResponse.transferId, pendingResponse.destNodeId);
        sendAckComplete(pendingResponse.channelIndex, pendingResponse.transferId, pendingResponse.destNodeId);
        if (completionCallback) {
            completionCallback(pendingResponse.data.data(), pendingResponse.totalSize,
                               pendingResponse.contentType, pendingResponse.destNodeId,
                               pendingResponse.transferId);
        }
        pendingResponse.data.clear();
        pendingResponse.data.shrink_to_fit();
        pendingResponse.type = PendingResponse::NONE;
    } else if (pendingResponse.type == PendingResponse::NACK) {
        LOG_INFO("MediaXfer: processing deferred NACK tid=%u to=0x%08x",
                 pendingResponse.transferId, pendingResponse.destNodeId);
        sendNack(pendingResponse.channelIndex, pendingResponse.transferId,
                 pendingResponse.destNodeId, pendingResponse.missingChunks);
        pendingResponse.missingChunks.clear();
        pendingResponse.type = PendingResponse::NONE;
    }

    if (!moduleConfig.has_media_transfer || !moduleConfig.media_transfer.enabled) {
        return disable();
    }

    uint32_t now = millis();

    // Process outgoing transfers — send next chunk
    for (auto it = outgoing.begin(); it != outgoing.end();) {
        OutgoingTransfer &xfer = *it;

        // Check for transfer timeout
        uint32_t maxTime = TRANSFER_TIMEOUT_MS;
        if (moduleConfig.media_transfer.max_transfer_time_minutes > 0) {
            maxTime = moduleConfig.media_transfer.max_transfer_time_minutes * 60000UL;
        }
        if (now - xfer.startTime > maxTime) {
            LOG_WARN("MediaXfer: outgoing transfer %u timed out", xfer.transferId);
            sendCancelPacket(xfer.transferId, xfer.channelIndex);
            it = outgoing.erase(it);
            continue;
        }

        // Send next chunk if enough time has passed (battery-throttled)
        uint32_t battMult = getBatteryRetryMultiplier();
        if (battMult == 0) {
            // Battery critically low — pause transfers, don't cancel
            ++it;
            continue;
        }
        uint32_t effectiveInterval = CHUNK_SEND_INTERVAL_MS * battMult;
        if (!xfer.complete && (now - xfer.lastChunkTime >= effectiveInterval)) {
            if (!xfer.nackedChunks.empty()) {
                // Retransmit NACKed chunks first
                uint32_t chunkIdx = xfer.nackedChunks.back();
                xfer.nackedChunks.pop_back();

                uint32_t chunkSize = getChunkSize();
                uint32_t offset = chunkIdx * chunkSize;
                uint32_t remaining = xfer.data.size() - offset;
                uint32_t thisChunkSize = (remaining < chunkSize) ? remaining : chunkSize;

                meshtastic_MediaTransfer pkt = meshtastic_MediaTransfer_init_zero;
                pkt.type = meshtastic_MediaTransferType_MEDIA_CHUNK;
                pkt.transfer_id = xfer.transferId;
                pkt.chunk_index = chunkIdx;
                pkt.chunk_data.size = thisChunkSize;
                memcpy(pkt.chunk_data.bytes, xfer.data.data() + offset, thisChunkSize);

                sendMediaPacket(xfer.channelIndex, xfer.destNodeId, pkt);
                xfer.lastChunkTime = now;
                LOG_DEBUG("MediaXfer: retransmit chunk %u/%u for transfer %u",
                         chunkIdx, xfer.totalChunks, xfer.transferId);
            } else if (xfer.nextChunkToSend < xfer.totalChunks) {
                sendNextChunk(xfer);
            } else if (!xfer.complete) {
                // All chunks sent, send COMPLETE
                xfer.complete = true;
                xfer.completeResendCount = 0;
                sendCompletePacket(xfer);
                LOG_INFO("MediaXfer: all chunks sent for transfer %u, waiting for ACK", xfer.transferId);
            }
        }

        // Resend COMPLETE periodically if no ACK/NACK received
        if (xfer.complete && xfer.nackedChunks.empty()) {
            if (xfer.completeResendCount < MAX_COMPLETE_RESENDS &&
                (now - xfer.lastChunkTime >= COMPLETE_RESEND_INTERVAL_MS)) {
                xfer.completeResendCount++;
                sendCompletePacket(xfer);
                LOG_INFO("MediaXfer: resending COMPLETE for transfer %u (attempt %u/%u)",
                         xfer.transferId, xfer.completeResendCount, MAX_COMPLETE_RESENDS);
            }
            // Final timeout after max resends exhausted
            else if (xfer.completeResendCount >= MAX_COMPLETE_RESENDS &&
                     (now - xfer.lastChunkTime > NACK_TIMEOUT_MS)) {
                LOG_INFO("MediaXfer: transfer %u finished (no ACK after %u COMPLETE resends)",
                         xfer.transferId, xfer.completeResendCount);
                it = outgoing.erase(it);
                continue;
            }
        }

        ++it;
    }

    // Clean up timed-out incoming transfers + proactive NACK for missing chunks
    for (auto it = incoming.begin(); it != incoming.end();) {
        if (now - it->lastChunkTime > TRANSFER_TIMEOUT_MS) {
            LOG_WARN("MediaXfer: incoming transfer %u timed out", it->transferId);
            it = incoming.erase(it);
        } else {
            // Proactive NACK: if we have some chunks but not all, and enough time
            // has passed since last chunk or last NACK, request missing chunks
            uint32_t lastActivity = (it->lastNackTime > it->lastChunkTime) ? it->lastNackTime : it->lastChunkTime;
            if (pendingResponse.type == PendingResponse::NONE &&
                now - lastActivity >= PROACTIVE_NACK_INTERVAL_MS) {
                std::vector<uint32_t> missing;
                for (uint32_t i = 0; i < it->totalChunks; i++) {
                    if (!it->receivedChunks[i]) {
                        missing.push_back(i);
                    }
                }
                if (!missing.empty()) {
                    LOG_INFO("MediaXfer: proactive NACK for transfer %u, %u/%u missing",
                             it->transferId, missing.size(), it->totalChunks);
                    pendingResponse.type = PendingResponse::NACK;
                    pendingResponse.channelIndex = it->channelIndex;
                    pendingResponse.transferId = it->transferId;
                    pendingResponse.destNodeId = it->fromNodeId;
                    pendingResponse.missingChunks = missing;
                    it->lastNackTime = now;
                    setIntervalFromNow(0);
                }
            }
            ++it;
        }
    }

    return 1000; // check every second
}

bool MediaTransferModule::handleReceivedProtobuf(const meshtastic_MeshPacket &mp, meshtastic_MediaTransfer *decoded)
{
    if (!decoded) {
        LOG_WARN("MediaXfer: handleReceivedProtobuf called with NULL decoded (from=0x%08x)", mp.from);
        return false;
    }

    LOG_DEBUG("MediaXfer: RX type=%d tid=%u from=0x%08x to=0x%08x chunkSz=%u",
             decoded->type, decoded->transfer_id, mp.from, mp.to,
             decoded->chunk_data.size);

    switch (decoded->type) {
    case meshtastic_MediaTransferType_MEDIA_START:
        handleMediaStart(mp, *decoded);
        break;
    case meshtastic_MediaTransferType_MEDIA_CHUNK:
        handleMediaChunk(mp, *decoded);
        // CONSUME chunks — do NOT forward each one to the phone/serial. Forwarding every
        // received chunk floods the USB-CDC, which drops bytes (setTxTimeoutMs) → corrupted
        // protobuf → the host loses sync ("serial LOST") → media transfers fail. The receiver
        // reassembles internally and delivers the COMPLETE media via the completion callback,
        // and the host measures success via ACK_COMPLETE — neither needs raw chunks on serial.
        return true;
    case meshtastic_MediaTransferType_MEDIA_COMPLETE:
        handleMediaComplete(mp, *decoded);
        break;
    case meshtastic_MediaTransferType_MEDIA_NACK:
        handleMediaNack(mp, *decoded);
        // Forward NACK to serial/phone for diagnostic visibility
        return false;
    case meshtastic_MediaTransferType_MEDIA_ACK_COMPLETE:
        handleMediaAckComplete(mp, *decoded);
        // Forward ACK_COMPLETE to serial/phone for delivery confirmation
        return false;
    case meshtastic_MediaTransferType_MEDIA_CANCEL:
        handleMediaCancel(mp, *decoded);
        return false;
    default:
        LOG_WARN("MediaXfer: unknown type %d", decoded->type);
        break;
    }

    // Forward START/CHUNK/COMPLETE to serial so test scripts can monitor reception
    return false;
}

uint32_t MediaTransferModule::startTransfer(uint32_t destNodeId, uint8_t channelIndex,
                                             const uint8_t *compressedData, uint32_t dataSize,
                                             meshtastic_MediaContentType contentType,
                                             uint32_t checksum, const char *mimeType,
                                             uint32_t durationSeconds, uint32_t width, uint32_t height)
{
    if (outgoing.size() >= MAX_CONCURRENT_TRANSFERS) {
        LOG_WARN("MediaXfer: too many concurrent transfers");
        return 0;
    }

    if (dataSize == 0 || dataSize > MAX_TRANSFER_SIZE) {
        LOG_WARN("MediaXfer: rejected outgoing transfer — size %u exceeds limit %u", dataSize, MAX_TRANSFER_SIZE);
        return 0;
    }

    uint32_t chunkSize = getChunkSize();
    uint32_t totalChunks = (dataSize + chunkSize - 1) / chunkSize;

    OutgoingTransfer xfer;
    xfer.transferId = generateTransferId();
    xfer.destNodeId = destNodeId;
    xfer.channelIndex = channelIndex;
    xfer.totalChunks = totalChunks;
    xfer.totalSize = dataSize;
    xfer.checksum = checksum;
    xfer.contentType = contentType;
    xfer.nextChunkToSend = 0;
    xfer.startTime = millis();
    xfer.lastChunkTime = 0;
    xfer.complete = false;
    xfer.completeResendCount = 0;
    xfer.data.assign(compressedData, compressedData + dataSize);

    // Send START packet
    sendStartPacket(xfer);

    uint32_t tid = xfer.transferId;
    outgoing.push_back(std::move(xfer));

    LOG_INFO("MediaXfer: started transfer %u, %u bytes in %u chunks to 0x%08x",
             tid, dataSize, totalChunks, destNodeId);
    return tid;
}

void MediaTransferModule::cancelTransfer(uint32_t transferId)
{
    for (auto it = outgoing.begin(); it != outgoing.end(); ++it) {
        if (it->transferId == transferId) {
            sendCancelPacket(transferId, it->channelIndex);
            outgoing.erase(it);
            LOG_INFO("MediaXfer: cancelled transfer %u", transferId);
            return;
        }
    }
}

int MediaTransferModule::getTransferProgress(uint32_t transferId)
{
    for (auto &xfer : outgoing) {
        if (xfer.transferId == transferId) {
            if (xfer.totalChunks == 0) return 0;
            return (xfer.nextChunkToSend * 100) / xfer.totalChunks;
        }
    }
    return -1; // not found
}

void MediaTransferModule::sendStartPacket(OutgoingTransfer &xfer)
{
    meshtastic_MediaTransfer pkt = meshtastic_MediaTransfer_init_zero;
    pkt.type = meshtastic_MediaTransferType_MEDIA_START;
    pkt.transfer_id = xfer.transferId;
    pkt.total_chunks = xfer.totalChunks;
    pkt.total_size = xfer.totalSize;
    pkt.content_type = xfer.contentType;
    pkt.checksum = xfer.checksum;

    sendMediaPacket(xfer.channelIndex, xfer.destNodeId, pkt);
    xfer.lastChunkTime = millis();
}

void MediaTransferModule::sendNextChunk(OutgoingTransfer &xfer)
{
    uint32_t chunkSize = getChunkSize();
    uint32_t offset = xfer.nextChunkToSend * chunkSize;
    uint32_t remaining = xfer.data.size() - offset;
    uint32_t thisChunkSize = (remaining < chunkSize) ? remaining : chunkSize;

    meshtastic_MediaTransfer pkt = meshtastic_MediaTransfer_init_zero;
    pkt.type = meshtastic_MediaTransferType_MEDIA_CHUNK;
    pkt.transfer_id = xfer.transferId;
    pkt.chunk_index = xfer.nextChunkToSend;
    pkt.chunk_data.size = thisChunkSize;
    memcpy(pkt.chunk_data.bytes, xfer.data.data() + offset, thisChunkSize);

    sendMediaPacket(xfer.channelIndex, xfer.destNodeId, pkt);
    xfer.lastChunkTime = millis();
    xfer.nextChunkToSend++;

    LOG_DEBUG("MediaXfer: sent chunk %u/%u (%u bytes) for transfer %u",
             xfer.nextChunkToSend - 1, xfer.totalChunks, thisChunkSize, xfer.transferId);
}

void MediaTransferModule::sendCompletePacket(OutgoingTransfer &xfer)
{
    meshtastic_MediaTransfer pkt = meshtastic_MediaTransfer_init_zero;
    pkt.type = meshtastic_MediaTransferType_MEDIA_COMPLETE;
    pkt.transfer_id = xfer.transferId;
    pkt.checksum = xfer.checksum;

    sendMediaPacket(xfer.channelIndex, xfer.destNodeId, pkt);
    xfer.lastChunkTime = millis();
}

void MediaTransferModule::sendCancelPacket(uint32_t transferId, uint8_t channelIndex)
{
    meshtastic_MediaTransfer pkt = meshtastic_MediaTransfer_init_zero;
    pkt.type = meshtastic_MediaTransferType_MEDIA_CANCEL;
    pkt.transfer_id = transferId;

    // Send as broadcast so both sender and receiver see it
    meshtastic_MeshPacket *p = allocDataProtobuf(pkt);
    p->to = NODENUM_BROADCAST;
    p->channel = channelIndex;
    p->want_ack = false;
    p->decoded.want_response = false;
    p->priority = meshtastic_MeshPacket_Priority_DEFAULT;
    service->sendToMesh(p);
}

void MediaTransferModule::handleMediaStart(const meshtastic_MeshPacket &mp, const meshtastic_MediaTransfer &decoded)
{
    LOG_INFO("MediaXfer: START from 0x%08x, transferId=%u, %u chunks, %u bytes, type=%d",
             mp.from, decoded.transfer_id, decoded.total_chunks, decoded.total_size, decoded.content_type);

    // Check if we already have this transfer
    for (auto &xfer : incoming) {
        if (xfer.transferId == decoded.transfer_id) {
            LOG_DEBUG("MediaXfer: duplicate START for transfer %u", decoded.transfer_id);
            return;
        }
    }

    if (incoming.size() >= MAX_CONCURRENT_TRANSFERS) {
        LOG_WARN("MediaXfer: too many concurrent incoming transfers, ignoring");
        return;
    }

    if (decoded.total_size == 0 || decoded.total_size > MAX_TRANSFER_SIZE) {
        LOG_WARN("MediaXfer: rejected transfer %u — size %u exceeds limit %u",
                 decoded.transfer_id, decoded.total_size, MAX_TRANSFER_SIZE);
        return;
    }

    if (decoded.total_chunks == 0 || decoded.total_chunks > (MAX_TRANSFER_SIZE / 32 + 1)) {
        LOG_WARN("MediaXfer: rejected transfer %u — chunk count %u invalid",
                 decoded.transfer_id, decoded.total_chunks);
        return;
    }

    // Check available heap before accepting — reject if insufficient RAM
    uint32_t freeHeap = ESP.getFreeHeap();
    uint32_t needed = decoded.total_size + decoded.total_chunks * sizeof(bool) + 1024; // data + bitmap + overhead
    if (freeHeap < needed * 2) { // require 2x headroom
        LOG_WARN("MediaXfer: rejected transfer %u — insufficient heap (free=%u, need=%u)",
                 decoded.transfer_id, freeHeap, needed * 2);
        return;
    }

    IncomingTransfer xfer;
    xfer.transferId = decoded.transfer_id;
    xfer.fromNodeId = mp.from;
    xfer.totalChunks = decoded.total_chunks;
    xfer.totalSize = decoded.total_size;
    xfer.checksum = decoded.checksum;
    xfer.contentType = decoded.content_type;
    xfer.channelIndex = mp.channel;
    xfer.startTime = millis();
    xfer.lastChunkTime = millis();
    xfer.lastNackTime = 0;
    xfer.receivedChunks.resize(decoded.total_chunks, false);
    xfer.data.resize(decoded.total_size, 0);
    strncpy(xfer.mimeType, decoded.mime_type, sizeof(xfer.mimeType) - 1);
    xfer.durationSeconds = decoded.duration_seconds;
    xfer.width = decoded.width;
    xfer.height = decoded.height;

    incoming.push_back(std::move(xfer));
}

void MediaTransferModule::handleMediaChunk(const meshtastic_MeshPacket &mp, const meshtastic_MediaTransfer &decoded)
{
    for (auto &xfer : incoming) {
        if (xfer.transferId == decoded.transfer_id) {
            if (decoded.chunk_index >= xfer.totalChunks) {
                LOG_WARN("MediaXfer: chunk index %u out of range for transfer %u",
                         decoded.chunk_index, decoded.transfer_id);
                return;
            }

            // Copy chunk data into reassembly buffer
            uint32_t chunkSize = getChunkSize();
            uint32_t offset = decoded.chunk_index * chunkSize;
            uint32_t copySize = decoded.chunk_data.size;
            if (offset + copySize > xfer.data.size()) {
                copySize = xfer.data.size() - offset;
            }
            memcpy(xfer.data.data() + offset, decoded.chunk_data.bytes, copySize);
            xfer.receivedChunks[decoded.chunk_index] = true;
            xfer.lastChunkTime = millis();

            // Count received
            uint32_t received = 0;
            for (bool b : xfer.receivedChunks) {
                if (b) received++;
            }
            LOG_DEBUG("MediaXfer: chunk %u/%u for transfer %u (%u/%u received)",
                     decoded.chunk_index, xfer.totalChunks, decoded.transfer_id, received, xfer.totalChunks);
            return;
        }
    }

    LOG_DEBUG("MediaXfer: chunk for unknown transfer %u", decoded.transfer_id);
}

void MediaTransferModule::handleMediaComplete(const meshtastic_MeshPacket &mp, const meshtastic_MediaTransfer &decoded)
{
    LOG_INFO("MediaXfer: COMPLETE from 0x%08x for transfer %u", mp.from, decoded.transfer_id);

    for (auto it = incoming.begin(); it != incoming.end(); ++it) {
        if (it->transferId == decoded.transfer_id) {
            // Check for missing chunks
            std::vector<uint32_t> missing;
            for (uint32_t i = 0; i < it->totalChunks; i++) {
                if (!it->receivedChunks[i]) {
                    missing.push_back(i);
                }
            }

            LOG_DEBUG("MediaXfer: COMPLETE tid=%u totalChunks=%u missing=%u", decoded.transfer_id, it->totalChunks, missing.size());
            if (!missing.empty()) {
                LOG_WARN("MediaXfer: %u missing chunks for transfer %u, deferring NACK to 0x%08x",
                         missing.size(), decoded.transfer_id, it->fromNodeId);
                // Defer NACK to runOnce() to avoid loopTask stack overflow
                pendingResponse.type = PendingResponse::NACK;
                pendingResponse.channelIndex = mp.channel;
                pendingResponse.transferId = decoded.transfer_id;
                pendingResponse.destNodeId = it->fromNodeId;
                pendingResponse.missingChunks = missing;
                setIntervalFromNow(0);
                return;
            }

            // All chunks received — verify checksum
            uint32_t computed = crc32(it->data.data(), it->data.size());
            if (computed != it->checksum) {
                LOG_WARN("MediaXfer: checksum mismatch for transfer %u (expected 0x%08x, got 0x%08x) from 0x%08x",
                         decoded.transfer_id, it->checksum, computed, it->fromNodeId);
                // Defer NACK to runOnce() to avoid loopTask stack overflow
                std::vector<uint32_t> allChunks;
                for (uint32_t i = 0; i < it->totalChunks; i++) allChunks.push_back(i);
                pendingResponse.type = PendingResponse::NACK;
                pendingResponse.channelIndex = mp.channel;
                pendingResponse.transferId = decoded.transfer_id;
                pendingResponse.destNodeId = it->fromNodeId;
                pendingResponse.missingChunks = std::move(allChunks);
                setIntervalFromNow(0);
                return;
            }

            LOG_INFO("MediaXfer: transfer %u complete! %u bytes, checksum OK",
                     decoded.transfer_id, it->totalSize);

            // Defer ACK_COMPLETE + completion callback to runOnce() to avoid
            // loopTask stack overflow (handleReceived chain is too deep for
            // protobuf encode + AES encrypt + queue + callback processing)
            pendingResponse.type = PendingResponse::ACK;
            pendingResponse.channelIndex = mp.channel;
            pendingResponse.transferId = decoded.transfer_id;
            pendingResponse.destNodeId = it->fromNodeId;
            pendingResponse.data = std::move(it->data);
            pendingResponse.totalSize = it->totalSize;
            pendingResponse.contentType = it->contentType;
            incoming.erase(it);
            setIntervalFromNow(0);
            return;
        }
    }

    LOG_DEBUG("MediaXfer: COMPLETE for unknown transfer %u", decoded.transfer_id);
}

void MediaTransferModule::handleMediaNack(const meshtastic_MeshPacket &mp, const meshtastic_MediaTransfer &decoded)
{
    LOG_INFO("MediaXfer: NACK from 0x%08x for transfer %u, %u missing chunks",
             mp.from, decoded.transfer_id, decoded.missing_chunks_count);

    for (auto &xfer : outgoing) {
        if (xfer.transferId == decoded.transfer_id) {
            // Queue the NACKed chunks for retransmission
            for (uint32_t i = 0; i < decoded.missing_chunks_count; i++) {
                xfer.nackedChunks.push_back(decoded.missing_chunks[i]);
            }
            xfer.complete = false; // reset complete flag to keep sending
            xfer.lastChunkTime = millis();
            return;
        }
    }
}

void MediaTransferModule::handleMediaAckComplete(const meshtastic_MeshPacket &mp, const meshtastic_MediaTransfer &decoded)
{
    LOG_INFO("MediaXfer: ACK_COMPLETE from 0x%08x for transfer %u", mp.from, decoded.transfer_id);

    for (auto it = outgoing.begin(); it != outgoing.end(); ++it) {
        if (it->transferId == decoded.transfer_id) {
            LOG_INFO("MediaXfer: transfer %u fully delivered!", decoded.transfer_id);
            outgoing.erase(it);
            return;
        }
    }
}

void MediaTransferModule::handleMediaCancel(const meshtastic_MeshPacket &mp, const meshtastic_MediaTransfer &decoded)
{
    LOG_INFO("MediaXfer: CANCEL from 0x%08x for transfer %u", mp.from, decoded.transfer_id);

    // Remove from outgoing
    for (auto it = outgoing.begin(); it != outgoing.end(); ++it) {
        if (it->transferId == decoded.transfer_id) {
            outgoing.erase(it);
            return;
        }
    }

    // Remove from incoming
    for (auto it = incoming.begin(); it != incoming.end(); ++it) {
        if (it->transferId == decoded.transfer_id) {
            incoming.erase(it);
            return;
        }
    }
}

void MediaTransferModule::sendNack(uint8_t channelIndex, uint32_t transferId,
                                    uint32_t destNodeId, const std::vector<uint32_t> &missingChunks)
{
    LOG_INFO("MediaXfer: SENDING NACK tid=%u to=0x%08x ch=%u missing=%u",
             transferId, destNodeId, channelIndex, missingChunks.size());

    meshtastic_MediaTransfer pkt = meshtastic_MediaTransfer_init_zero;
    pkt.type = meshtastic_MediaTransferType_MEDIA_NACK;
    pkt.transfer_id = transferId;
    pkt.missing_chunks_count = (missingChunks.size() > 32) ? 32 : missingChunks.size();
    for (uint32_t i = 0; i < pkt.missing_chunks_count; i++) {
        pkt.missing_chunks[i] = missingChunks[i];
    }

    // Send unicast to original sender (unicast passes sendToPhone filter)
    meshtastic_MeshPacket *p = allocDataProtobuf(pkt);
    p->to = destNodeId;
    p->channel = channelIndex;
    p->want_ack = false;
    p->decoded.want_response = false;
    p->priority = meshtastic_MeshPacket_Priority_ACK;
    service->sendToMesh(p);
}

void MediaTransferModule::sendAckComplete(uint8_t channelIndex, uint32_t transferId, uint32_t destNodeId)
{
    LOG_INFO("MediaXfer: SENDING ACK_COMPLETE tid=%u to=0x%08x ch=%u", transferId, destNodeId, channelIndex);

    meshtastic_MediaTransfer pkt = meshtastic_MediaTransfer_init_zero;
    pkt.type = meshtastic_MediaTransferType_MEDIA_ACK_COMPLETE;
    pkt.transfer_id = transferId;

    meshtastic_MeshPacket *p = allocDataProtobuf(pkt);
    p->to = destNodeId;
    p->channel = channelIndex;
    p->want_ack = true;  // ReliableRouter will retry if ACK_COMPLETE is lost
    p->decoded.want_response = false;
    p->priority = meshtastic_MeshPacket_Priority_RELIABLE;
    service->sendToMesh(p);
    LOG_DEBUG("MediaXfer: ACK_COMPLETE queued for mesh send (want_ack=true)");
}

void MediaTransferModule::sendMediaPacket(uint8_t channelIndex, uint32_t destNodeId,
                                           const meshtastic_MediaTransfer &payload)
{
    meshtastic_MeshPacket *p = allocDataProtobuf(payload);
    p->to = destNodeId;
    p->channel = channelIndex;
    p->want_ack = false;
    p->decoded.want_response = false;
    // Media transfers need at least RELIABLE priority to avoid being pushed behind
    // MQTT-relayed text messages (which get HIGH=73). With DEFAULT=64, media chunks
    // lose to MQTT text in the TX queue, causing packet loss and transfer failures.
    if (moduleConfig.has_media_transfer && moduleConfig.media_transfer.yield_to_text) {
        p->priority = meshtastic_MeshPacket_Priority_DEFAULT;
    } else {
        p->priority = meshtastic_MeshPacket_Priority_RELIABLE;
    }
    service->sendToMesh(p);
}

// Simple CRC32 implementation (IEEE 802.3)
uint32_t MediaTransferModule::crc32(const uint8_t *data, uint32_t length)
{
    uint32_t crc = 0xFFFFFFFF;
    for (uint32_t i = 0; i < length; i++) {
        crc ^= data[i];
        for (int j = 0; j < 8; j++) {
            if (crc & 1)
                crc = (crc >> 1) ^ 0xEDB88320;
            else
                crc >>= 1;
        }
    }
    return ~crc;
}
