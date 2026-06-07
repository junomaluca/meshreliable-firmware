#pragma once

#include "concurrency/OSThread.h"
#include "configuration.h"

#ifdef ARCH_ESP32
#include <codec2.h>
#include <map>
#include <vector>

class VoiceMemoModule : private concurrency::OSThread
{
  public:
    VoiceMemoModule();
    ~VoiceMemoModule();

    // Start recording a voice memo (max 60s)
    bool startRecording(uint32_t maxDurationMs = 30000);

    // Stop recording and send to a destination node
    // Returns transfer_id from MediaTransferModule, or 0 on failure
    uint32_t stopAndSend(uint32_t destNodeId, uint8_t channelIndex = 0);

    // Cancel recording without sending
    void cancelRecording();

    // Play Codec2-encoded audio data through the speaker
    bool playVoiceMemo(const uint8_t *data, uint32_t size);

    // Stop current playback
    void stopPlayback();

    // Called by MediaTransferModule when a VOICE_MEMO transfer completes
    void onTransferComplete(const uint8_t *data, uint32_t size, uint32_t fromNode, uint32_t transferId);

    // Generate a synthetic test memo (440 Hz tone) and send via MediaTransfer
    // Works on devices without audio hardware for pipeline testing
    uint32_t generateAndSendTestMemo(uint32_t destNodeId, uint8_t channelIndex = 0,
                                      uint32_t durationMs = 5000);

    bool isRecording() const { return state == State::RECORDING; }
    bool isPlaying() const { return state == State::PLAYING; }
    float getRecordingDuration() const;

    // Received voice memo storage for on-device playback
    struct ReceivedMemo {
        std::vector<uint8_t> data;
        uint32_t fromNode;
        uint32_t timestamp;
    };
    const ReceivedMemo *getMemo(uint32_t transferId) const;

  protected:
    int32_t runOnce() override;

  private:
    enum class State { IDLE, RECORDING, PLAYING, FORWARDING_TO_PHONE };
    State state = State::IDLE;

    // Codec2
    struct CODEC2 *codec2 = nullptr;
    int codecBytesPerFrame = 0;
    int samplesPerFrame = 0;

    // Recording
    std::vector<uint8_t> recordBuffer;
    uint32_t recordStartTime = 0;
    uint32_t maxRecordMs = 30000;

    // Playback
    std::vector<uint8_t> playBuffer;
    uint32_t playOffset = 0;

    // PCM scratch buffer (one frame)
    int16_t *pcmBuffer = nullptr;

    // Received voice memo storage (keyed by transferId, last N kept in RAM)
    std::map<uint32_t, ReceivedMemo> receivedMemos;
    static constexpr int MAX_STORED_MEMOS = 5;

    // Deferred transfer completion (heavy work deferred from callback to runOnce)
    struct PendingCompletion {
        std::vector<uint8_t> data;
        uint32_t fromNode = 0;
        uint32_t transferId = 0;
        bool needsPlayback = false;
        bool needsPhoneForward = false;
    };
    PendingCompletion pendingCompletion;

    // Send a batch of phone-forward chunks; returns true if more remain
    bool sendPhoneForwardBatch();

  public:
    // Streaming phone forward state (avoids blocking main thread).
    // Public so the static forwardImageToPhone helper can queue image data.
    struct PhoneForwardState {
        std::vector<uint8_t> pcmData;     // decoded PCM (voice) or raw data (image)
        uint32_t fromNode = 0;
        uint32_t transferId = 0;
        uint32_t totalChunks = 0;
        uint32_t nextChunk = 0;
        uint32_t checksum = 0;
        bool sentStart = false;
        bool isImage = false;             // true = image (0x04-0x06), false = voice (0x01-0x03)
        bool active = false;

        void reset() {
            pcmData.clear();
            pcmData.shrink_to_fit();
            nextChunk = 0;
            sentStart = false;
            active = false;
        }
    };
    PhoneForwardState phoneForward;

  private:

#ifdef HAS_VOICE_MEMO
    bool micInitialized = false;
    bool spkInitialized = false;

#ifndef VOICE_MEMO_ES8311
    // MVSR hardware (T3-S3 V1): separate MEMS mic + MAX98357A amp
    enum class MicMode { I2S_STANDARD, PDM };
    MicMode micMode = MicMode::I2S_STANDARD;
    bool micEnableInverted = false; // V1.1 PDM board has inverted MIC_EN
    bool initMicI2S();
    bool initMicPDM();
    void setMicEnable(bool enable);
#endif

    void initMic();
    void deinitMic();
    void initSpeaker();
    void deinitSpeaker();
    bool captureAndEncodeFrame();
    bool decodeAndPlayFrame();
#endif

    void initCodec2();
    void deinitCodec2();
    void encodeFrame(const int16_t *pcm);
    void forwardToPhone(const uint8_t *codec2Data, uint32_t size, uint32_t fromNode, uint32_t transferId);

  public:
    static uint32_t crc32(const uint8_t *data, uint32_t length);
};

extern VoiceMemoModule *voiceMemoModule;

#endif // ARCH_ESP32
