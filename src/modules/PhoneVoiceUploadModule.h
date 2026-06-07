#pragma once

#include "MeshModule.h"
#include "configuration.h"

#ifdef ARCH_ESP32
#include <codec2.h>
#include <vector>

/**
 * PhoneVoiceUploadModule
 *
 * Intercepts voice memo binary headers from the connected phone arriving
 * on PRIVATE_APP (portnum 256). Receives raw PCM audio at fast BLE speed,
 * incrementally Codec2-encodes each frame, then hands the compressed data
 * to MediaTransferModule for LoRa transmission.
 *
 * Returns ProcessMessage::CONTINUE for non-voice-memo packets so existing
 * privateApp relay is unaffected.
 *
 * Memory: ~11KB max for a 60s memo (640B PCM buffer + ~10.5KB Codec2 output)
 */
class PhoneVoiceUploadModule : public MeshModule
{
  public:
    PhoneVoiceUploadModule();
    virtual ~PhoneVoiceUploadModule();

  protected:
    virtual ProcessMessage handleReceived(const meshtastic_MeshPacket &mp) override;
    virtual bool wantPacket(const meshtastic_MeshPacket *p) override;

  private:
    // State for an active phone upload
    struct PhoneUpload {
        uint32_t transferId = 0;
        uint32_t destNode = 0;
        uint8_t destChannel = 0;
        uint32_t totalChunks = 0;
        uint32_t receivedChunks = 0;
        uint32_t expectedChecksum = 0;

        // PCM frame accumulator (320 samples = 640 bytes for one Codec2 frame)
        int16_t pcmFrameBuffer[320];
        uint32_t pcmFrameOffset = 0; // samples accumulated so far

        // Codec2 output (incremental encoding)
        std::vector<uint8_t> encodedData;
        uint32_t startTime = 0;

        bool active = false;
    };

    PhoneUpload upload;

    // Codec2 instance for encoding
    struct CODEC2 *codec2 = nullptr;
    int codecBytesPerFrame = 0;
    int samplesPerFrame = 0;

    void initCodec2();
    void deinitCodec2();
    void encodeAccumulatedPCM();
    void flushRemainingPCM();
    void finishUpload();
    void resetUpload();

    // Binary header parsing (matches iOS MediaTransferHeader)
    static constexpr int HEADER_SIZE = 13;
    enum HeaderType : uint8_t {
        VOICE_MEMO_START = 0x01,
        VOICE_MEMO_CHUNK = 0x02,
        VOICE_MEMO_END   = 0x03,
    };

    static uint32_t crc32(const uint8_t *data, uint32_t length);
};

extern PhoneVoiceUploadModule *phoneVoiceUploadModule;

#endif // ARCH_ESP32
