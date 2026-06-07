#include "configuration.h"
#ifdef ARCH_ESP32

#include "PhoneVoiceUploadModule.h"
#include "modules/MediaTransferModule.h"
#include "MeshService.h"
#include "NodeDB.h"

// !! CRITICAL — DO NOT CHANGE THIS BITRATE !!
// Voice memos MUST be compressed to Codec2 1300 bps before LoRa transmission.
// This reduces a 5-second memo from ~80KB (raw PCM) to ~812 bytes (Codec2).
// Without compression, voice memos would take minutes to transmit over LoRa.
// Previously regressed — this is a hard requirement. See also VoiceMemoModule.cpp.
// Codec2 1300 mode: 52 bits/frame (7 bytes), 320 samples/frame, 40ms frames
#define PHONE_UPLOAD_CODEC2_MODE CODEC2_MODE_1300

PhoneVoiceUploadModule *phoneVoiceUploadModule = nullptr;

PhoneVoiceUploadModule::PhoneVoiceUploadModule()
    : MeshModule("PhoneVoiceUpload")
{
    LOG_INFO("PhoneVoiceUpload: module created");
}

PhoneVoiceUploadModule::~PhoneVoiceUploadModule()
{
    deinitCodec2();
}

bool PhoneVoiceUploadModule::wantPacket(const meshtastic_MeshPacket *p)
{
    // Only intercept PRIVATE_APP packets addressed to us (from the phone via BLE)
    return p && p->decoded.portnum == meshtastic_PortNum_PRIVATE_APP &&
           (p->to == myNodeInfo.my_node_num || p->to == 0);
}

ProcessMessage PhoneVoiceUploadModule::handleReceived(const meshtastic_MeshPacket &mp)
{
    const uint8_t *data = mp.decoded.payload.bytes;
    uint32_t len = mp.decoded.payload.size;

    // Must have at least the binary header
    if (len < (uint32_t)HEADER_SIZE)
        return ProcessMessage::CONTINUE;

    // Parse binary header: [type:1][transferId:4][seqNum:2][totalChunks:2][checksum:4]
    uint8_t msgType = data[0];

    // Only handle voice memo types — let everything else pass through
    if (msgType != VOICE_MEMO_START && msgType != VOICE_MEMO_CHUNK && msgType != VOICE_MEMO_END)
        return ProcessMessage::CONTINUE;

    uint32_t transferId;
    uint16_t seqNum, totalChunks;
    uint32_t checksum;
    memcpy(&transferId, data + 1, 4); transferId = __builtin_bswap32(__builtin_bswap32(transferId)); // LE
    memcpy(&seqNum, data + 5, 2);
    memcpy(&totalChunks, data + 7, 2);
    memcpy(&checksum, data + 9, 4);
    // Ensure little-endian on ESP32 (already LE, but be explicit)

    const uint8_t *payload = data + HEADER_SIZE;
    uint32_t payloadLen = len - HEADER_SIZE;

    switch (msgType) {
    case VOICE_MEMO_START: {
        // Payload contains: [destNode:4 LE][destChannel:1]
        if (payloadLen < 5) {
            LOG_WARN("PhoneVoiceUpload: START payload too small (%u)", payloadLen);
            return ProcessMessage::STOP;
        }

        resetUpload();

        uint32_t destNode;
        memcpy(&destNode, payload, 4);
        uint8_t destChannel = payload[4];

        upload.transferId = transferId;
        upload.destNode = destNode;
        upload.destChannel = destChannel;
        upload.totalChunks = totalChunks;
        upload.expectedChecksum = checksum;
        upload.startTime = millis();
        upload.active = true;

        if (!codec2) initCodec2();

        LOG_INFO("PhoneVoiceUpload: START tid=%u dest=0x%08x ch=%u chunks=%u",
                 transferId, destNode, destChannel, totalChunks);
        return ProcessMessage::STOP;
    }

    case VOICE_MEMO_CHUNK: {
        if (!upload.active || upload.transferId != transferId) {
            LOG_WARN("PhoneVoiceUpload: CHUNK for unknown transfer %u", transferId);
            return ProcessMessage::STOP;
        }

        // Payload is raw PCM Int16 samples
        uint32_t sampleCount = payloadLen / sizeof(int16_t);
        const int16_t *samples = (const int16_t *)payload;

        for (uint32_t i = 0; i < sampleCount; i++) {
            upload.pcmFrameBuffer[upload.pcmFrameOffset++] = samples[i];

            // When we have a full Codec2 frame (320 samples), encode it
            if (upload.pcmFrameOffset >= (uint32_t)samplesPerFrame) {
                encodeAccumulatedPCM();
            }
        }

        upload.receivedChunks++;

        if (upload.receivedChunks % 50 == 0) {
            LOG_INFO("PhoneVoiceUpload: chunk %u/%u, encoded %u bytes so far",
                     upload.receivedChunks, upload.totalChunks, upload.encodedData.size());
        }
        return ProcessMessage::STOP;
    }

    case VOICE_MEMO_END: {
        if (!upload.active || upload.transferId != transferId) {
            LOG_WARN("PhoneVoiceUpload: END for unknown transfer %u", transferId);
            return ProcessMessage::STOP;
        }

        LOG_INFO("PhoneVoiceUpload: END received. %u chunks, %u PCM samples remaining",
                 upload.receivedChunks, upload.pcmFrameOffset);

        // Encode any remaining PCM samples (partial frame, zero-pad)
        flushRemainingPCM();

        // Hand off to MediaTransferModule for LoRa transmission
        finishUpload();
        return ProcessMessage::STOP;
    }

    default:
        return ProcessMessage::CONTINUE;
    }
}

void PhoneVoiceUploadModule::initCodec2()
{
    if (codec2) return;

    codec2 = codec2_create(PHONE_UPLOAD_CODEC2_MODE);
    if (!codec2) {
        LOG_ERROR("PhoneVoiceUpload: Failed to create Codec2");
        return;
    }

    codec2_set_lpc_post_filter(codec2, 1, 0, 0.8, 0.2);
    codecBytesPerFrame = (codec2_bits_per_frame(codec2) + 7) / 8;
    samplesPerFrame = codec2_samples_per_frame(codec2);

    LOG_INFO("PhoneVoiceUpload: Codec2 mode 1300 — %d bytes/frame, %d samples/frame",
             codecBytesPerFrame, samplesPerFrame);
}

void PhoneVoiceUploadModule::deinitCodec2()
{
    if (codec2) {
        codec2_destroy(codec2);
        codec2 = nullptr;
    }
}

void PhoneVoiceUploadModule::encodeAccumulatedPCM()
{
    if (!codec2 || upload.pcmFrameOffset < (uint32_t)samplesPerFrame) return;

    size_t prev = upload.encodedData.size();
    upload.encodedData.resize(prev + codecBytesPerFrame);
    codec2_encode(codec2, upload.encodedData.data() + prev, upload.pcmFrameBuffer);
    upload.pcmFrameOffset = 0;
}

void PhoneVoiceUploadModule::flushRemainingPCM()
{
    if (!codec2 || upload.pcmFrameOffset == 0) return;

    // Zero-pad the remaining samples to fill a complete frame
    while (upload.pcmFrameOffset < (uint32_t)samplesPerFrame) {
        upload.pcmFrameBuffer[upload.pcmFrameOffset++] = 0;
    }
    encodeAccumulatedPCM();
}

void PhoneVoiceUploadModule::finishUpload()
{
    if (upload.encodedData.empty()) {
        LOG_WARN("PhoneVoiceUpload: No encoded data to send");
        resetUpload();
        return;
    }

    if (!mediaTransferModule) {
        LOG_ERROR("PhoneVoiceUpload: No MediaTransferModule available");
        resetUpload();
        return;
    }

    uint32_t elapsedMs = millis() - upload.startTime;
    uint32_t durationSec = upload.receivedChunks * 200 / (2 * 8000); // PCM bytes -> seconds
    // Better estimate: total PCM bytes = receivedChunks * ~200, samples = bytes/2, duration = samples/8000
    if (durationSec == 0) durationSec = 1;

    uint32_t encodedChecksum = crc32(upload.encodedData.data(), upload.encodedData.size());

    LOG_INFO("PhoneVoiceUpload: Codec2 encoding complete — %u bytes encoded from ~%u PCM chunks in %u ms",
             upload.encodedData.size(), upload.receivedChunks, elapsedMs);
    LOG_INFO("PhoneVoiceUpload: Starting LoRa transfer to 0x%08x ch=%u (~%u sec audio)",
             upload.destNode, upload.destChannel, durationSec);

    uint32_t tid = mediaTransferModule->startTransfer(
        upload.destNode, upload.destChannel,
        upload.encodedData.data(), upload.encodedData.size(),
        meshtastic_MediaContentType_VOICE_MEMO,
        encodedChecksum, "audio/codec2", durationSec);

    if (tid > 0) {
        LOG_INFO("PhoneVoiceUpload: MediaTransfer started tid=%u", tid);
    } else {
        LOG_ERROR("PhoneVoiceUpload: MediaTransfer start failed");
    }

    resetUpload();
}

void PhoneVoiceUploadModule::resetUpload()
{
    upload.active = false;
    upload.transferId = 0;
    upload.destNode = 0;
    upload.destChannel = 0;
    upload.totalChunks = 0;
    upload.receivedChunks = 0;
    upload.expectedChecksum = 0;
    upload.pcmFrameOffset = 0;
    upload.encodedData.clear();
    upload.encodedData.shrink_to_fit();
    upload.startTime = 0;
}

uint32_t PhoneVoiceUploadModule::crc32(const uint8_t *data, uint32_t length)
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

#endif // ARCH_ESP32
