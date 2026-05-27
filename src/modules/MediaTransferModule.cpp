#include "MediaTransferModule.h"
#include "MeshService.h"
#include "NodeDB.h"
#include "Router.h"
#include "configuration.h"
#include "gps/RTC.h"

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

        // Send next chunk if enough time has passed
        if (!xfer.complete && (now - xfer.lastChunkTime >= CHUNK_SEND_INTERVAL_MS)) {
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
                pkt.chunk_data_size = thisChunkSize;
                memcpy(pkt.chunk_data, xfer.data.data() + offset, thisChunkSize);

                sendMediaPacket(xfer.channelIndex, xfer.destNodeId, pkt);
                xfer.lastChunkTime = now;
                LOG_DEBUG("MediaXfer: retransmit chunk %u/%u for transfer %u",
                         chunkIdx, xfer.totalChunks, xfer.transferId);
            } else if (xfer.nextChunkToSend < xfer.totalChunks) {
                sendNextChunk(xfer);
            } else if (!xfer.complete) {
                // All chunks sent, send COMPLETE
                xfer.complete = true;
                sendCompletePacket(xfer);
                LOG_INFO("MediaXfer: all chunks sent for transfer %u, waiting for ACK", xfer.transferId);
            }
        }

        // If complete and no more NACKed chunks, wait for ACK_COMPLETE or timeout
        if (xfer.complete && xfer.nackedChunks.empty() &&
            (now - xfer.lastChunkTime > NACK_TIMEOUT_MS)) {
            LOG_INFO("MediaXfer: transfer %u finished (no more NACKs)", xfer.transferId);
            it = outgoing.erase(it);
            continue;
        }

        ++it;
    }

    // Clean up timed-out incoming transfers
    for (auto it = incoming.begin(); it != incoming.end();) {
        if (now - it->lastChunkTime > TRANSFER_TIMEOUT_MS) {
            LOG_WARN("MediaXfer: incoming transfer %u timed out", it->transferId);
            it = incoming.erase(it);
        } else {
            ++it;
        }
    }

    return 1000; // check every second
}

bool MediaTransferModule::handleReceivedProtobuf(const meshtastic_MeshPacket &mp, meshtastic_MediaTransfer *decoded)
{
    if (!decoded) return false;

    switch (decoded->type) {
    case meshtastic_MediaTransferType_MEDIA_START:
        handleMediaStart(mp, *decoded);
        break;
    case meshtastic_MediaTransferType_MEDIA_CHUNK:
        handleMediaChunk(mp, *decoded);
        break;
    case meshtastic_MediaTransferType_MEDIA_COMPLETE:
        handleMediaComplete(mp, *decoded);
        break;
    case meshtastic_MediaTransferType_MEDIA_NACK:
        handleMediaNack(mp, *decoded);
        break;
    case meshtastic_MediaTransferType_MEDIA_ACK_COMPLETE:
        handleMediaAckComplete(mp, *decoded);
        break;
    case meshtastic_MediaTransferType_MEDIA_CANCEL:
        handleMediaCancel(mp, *decoded);
        break;
    default:
        LOG_WARN("MediaXfer: unknown type %d", decoded->type);
        break;
    }

    return true;
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
    pkt.chunk_data_size = thisChunkSize;
    memcpy(pkt.chunk_data, xfer.data.data() + offset, thisChunkSize);

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

    IncomingTransfer xfer;
    xfer.transferId = decoded.transfer_id;
    xfer.fromNodeId = mp.from;
    xfer.totalChunks = decoded.total_chunks;
    xfer.totalSize = decoded.total_size;
    xfer.checksum = decoded.checksum;
    xfer.contentType = decoded.content_type;
    xfer.startTime = millis();
    xfer.lastChunkTime = millis();
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
            uint32_t copySize = decoded.chunk_data_size;
            if (offset + copySize > xfer.data.size()) {
                copySize = xfer.data.size() - offset;
            }
            memcpy(xfer.data.data() + offset, decoded.chunk_data, copySize);
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

            if (!missing.empty()) {
                LOG_INFO("MediaXfer: %u missing chunks for transfer %u, sending NACK",
                         missing.size(), decoded.transfer_id);
                sendNack(mp.channel, decoded.transfer_id, missing);
                return;
            }

            // All chunks received — verify checksum
            uint32_t computed = crc32(it->data.data(), it->data.size());
            if (computed != it->checksum) {
                LOG_WARN("MediaXfer: checksum mismatch for transfer %u (expected 0x%08x, got 0x%08x)",
                         decoded.transfer_id, it->checksum, computed);
                // Request full retransmission by NACKing all chunks
                std::vector<uint32_t> allChunks;
                for (uint32_t i = 0; i < it->totalChunks; i++) allChunks.push_back(i);
                sendNack(mp.channel, decoded.transfer_id, allChunks);
                return;
            }

            LOG_INFO("MediaXfer: transfer %u complete! %u bytes, checksum OK",
                     decoded.transfer_id, it->totalSize);
            sendAckComplete(mp.channel, decoded.transfer_id);

            // The complete data is in it->data — available for the companion app to retrieve
            // For now, just log completion. Future: expose via PhoneAPI
            incoming.erase(it);
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
                                    const std::vector<uint32_t> &missingChunks)
{
    meshtastic_MediaTransfer pkt = meshtastic_MediaTransfer_init_zero;
    pkt.type = meshtastic_MediaTransferType_MEDIA_NACK;
    pkt.transfer_id = transferId;
    pkt.missing_chunks_count = (missingChunks.size() > 32) ? 32 : missingChunks.size();
    for (uint32_t i = 0; i < pkt.missing_chunks_count; i++) {
        pkt.missing_chunks[i] = missingChunks[i];
    }

    // Send as broadcast (sender will pick it up)
    meshtastic_MeshPacket *p = allocDataProtobuf(pkt);
    p->to = NODENUM_BROADCAST;
    p->channel = channelIndex;
    p->want_ack = false;
    p->decoded.want_response = false;
    p->priority = meshtastic_MeshPacket_Priority_DEFAULT;
    service->sendToMesh(p);
}

void MediaTransferModule::sendAckComplete(uint8_t channelIndex, uint32_t transferId)
{
    meshtastic_MediaTransfer pkt = meshtastic_MediaTransfer_init_zero;
    pkt.type = meshtastic_MediaTransferType_MEDIA_ACK_COMPLETE;
    pkt.transfer_id = transferId;

    meshtastic_MeshPacket *p = allocDataProtobuf(pkt);
    p->to = NODENUM_BROADCAST;
    p->channel = channelIndex;
    p->want_ack = false;
    p->decoded.want_response = false;
    p->priority = meshtastic_MeshPacket_Priority_DEFAULT;
    service->sendToMesh(p);
}

void MediaTransferModule::sendMediaPacket(uint8_t channelIndex, uint32_t destNodeId,
                                           const meshtastic_MediaTransfer &payload)
{
    meshtastic_MeshPacket *p = allocDataProtobuf(payload);
    p->to = destNodeId;
    p->channel = channelIndex;
    p->want_ack = false;
    p->decoded.want_response = false;
    // Media transfers use lower priority than text
    if (moduleConfig.has_media_transfer && moduleConfig.media_transfer.yield_to_text) {
        p->priority = meshtastic_MeshPacket_Priority_BACKGROUND;
    } else {
        p->priority = meshtastic_MeshPacket_Priority_DEFAULT;
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
