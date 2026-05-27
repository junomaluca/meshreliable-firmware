#pragma once

#include "concurrency/OSThread.h"
#include "configuration.h"

#ifdef ARCH_ESP32
#include <codec2.h>
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
    void onTransferComplete(const uint8_t *data, uint32_t size, uint32_t fromNode);

    // Generate a synthetic test memo (440 Hz tone) and send via MediaTransfer
    // Works on devices without audio hardware for pipeline testing
    uint32_t generateAndSendTestMemo(uint32_t destNodeId, uint8_t channelIndex = 0,
                                      uint32_t durationMs = 5000);

    bool isRecording() const { return state == State::RECORDING; }
    bool isPlaying() const { return state == State::PLAYING; }
    float getRecordingDuration() const;

  protected:
    int32_t runOnce() override;

  private:
    enum class State { IDLE, RECORDING, PLAYING };
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

#ifdef HAS_VOICE_MEMO
    bool micInitialized = false;
    bool spkInitialized = false;

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
    static uint32_t crc32(const uint8_t *data, uint32_t length);
};

extern VoiceMemoModule *voiceMemoModule;

#endif // ARCH_ESP32
