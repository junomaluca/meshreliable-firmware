#include "configuration.h"
#ifdef ARCH_ESP32

#include "VoiceMemoModule.h"
#include "modules/MediaTransferModule.h"
#include "MeshService.h"
#include "NodeDB.h"

#ifdef HAS_VOICE_MEMO
#include <driver/i2s.h>

#ifndef MVSR_MIC_I2S_PORT
#define MVSR_MIC_I2S_PORT I2S_NUM_0
#endif
#ifndef MVSR_SPK_I2S_PORT
#define MVSR_SPK_I2S_PORT I2S_NUM_1
#endif
#endif // HAS_VOICE_MEMO

// Codec2 700 mode: 28 bits/frame (4 bytes), 320 samples/frame, 40ms frames
#define VOICE_MEMO_CODEC2_MODE CODEC2_MODE_700

VoiceMemoModule *voiceMemoModule = nullptr;

VoiceMemoModule::VoiceMemoModule()
    : concurrency::OSThread("VoiceMemo")
{
    initCodec2();

    // Register with MediaTransferModule for voice memo completion callbacks
    if (mediaTransferModule) {
        mediaTransferModule->setTransferCompleteCallback(
            [](const uint8_t *data, uint32_t size, meshtastic_MediaContentType type,
               uint32_t fromNode, uint32_t transferId) {
                if (type == meshtastic_MediaContentType_VOICE_MEMO && voiceMemoModule) {
                    voiceMemoModule->onTransferComplete(data, size, fromNode);
                }
            });
    }
}

VoiceMemoModule::~VoiceMemoModule()
{
    deinitCodec2();
#ifdef HAS_VOICE_MEMO
    deinitMic();
    deinitSpeaker();
#endif
}

void VoiceMemoModule::initCodec2()
{
    if (codec2) return;

    codec2 = codec2_create(VOICE_MEMO_CODEC2_MODE);
    if (!codec2) {
        LOG_ERROR("VoiceMemo: Failed to create Codec2");
        return;
    }

    codec2_set_lpc_post_filter(codec2, 1, 0, 0.8, 0.2);
    codecBytesPerFrame = (codec2_bits_per_frame(codec2) + 7) / 8;
    samplesPerFrame = codec2_samples_per_frame(codec2);
    pcmBuffer = new int16_t[samplesPerFrame];

    LOG_INFO("VoiceMemo: Codec2 mode %d — %d bytes/frame, %d samples/frame",
             VOICE_MEMO_CODEC2_MODE, codecBytesPerFrame, samplesPerFrame);
}

void VoiceMemoModule::deinitCodec2()
{
    if (codec2) {
        codec2_destroy(codec2);
        codec2 = nullptr;
    }
    delete[] pcmBuffer;
    pcmBuffer = nullptr;
}

// --- I2S Mic / Speaker (MVSR hardware only) ---

#ifdef HAS_VOICE_MEMO

void VoiceMemoModule::initMic()
{
    if (micInitialized) return;

    pinMode(MVSR_MIC_EN, OUTPUT);
    digitalWrite(MVSR_MIC_EN, HIGH);
    delay(10);

    i2s_config_t cfg = {};
    cfg.mode = (i2s_mode_t)(I2S_MODE_MASTER | I2S_MODE_RX);
    cfg.sample_rate = 8000;
    cfg.bits_per_sample = I2S_BITS_PER_SAMPLE_16BIT;
    cfg.channel_format = I2S_CHANNEL_FMT_ONLY_LEFT;
    cfg.communication_format = I2S_COMM_FORMAT_STAND_I2S;
    cfg.intr_alloc_flags = 0;
    cfg.dma_buf_count = 8;
    cfg.dma_buf_len = samplesPerFrame;
    cfg.use_apll = false;

    esp_err_t err = i2s_driver_install(MVSR_MIC_I2S_PORT, &cfg, 0, NULL);
    if (err != ESP_OK) {
        LOG_ERROR("VoiceMemo: mic I2S install failed: %d", err);
        return;
    }

    i2s_pin_config_t pins = {};
    pins.bck_io_num = MVSR_MIC_BCLK;
    pins.ws_io_num = MVSR_MIC_WS;
    pins.data_out_num = I2S_PIN_NO_CHANGE;
    pins.data_in_num = MVSR_MIC_DATA;

    err = i2s_set_pin(MVSR_MIC_I2S_PORT, &pins);
    if (err != ESP_OK) {
        LOG_ERROR("VoiceMemo: mic pin config failed: %d", err);
        i2s_driver_uninstall(MVSR_MIC_I2S_PORT);
        return;
    }

    i2s_start(MVSR_MIC_I2S_PORT);
    micInitialized = true;
    LOG_INFO("VoiceMemo: Mic initialized (BCLK=%d WS=%d DATA=%d EN=%d)",
             MVSR_MIC_BCLK, MVSR_MIC_WS, MVSR_MIC_DATA, MVSR_MIC_EN);
}

void VoiceMemoModule::deinitMic()
{
    if (!micInitialized) return;
    i2s_stop(MVSR_MIC_I2S_PORT);
    i2s_driver_uninstall(MVSR_MIC_I2S_PORT);
    digitalWrite(MVSR_MIC_EN, LOW);
    micInitialized = false;
}

void VoiceMemoModule::initSpeaker()
{
    if (spkInitialized) return;

    pinMode(MVSR_SPK_SD_MODE, OUTPUT);
    digitalWrite(MVSR_SPK_SD_MODE, HIGH);
    delay(10);

    i2s_config_t cfg = {};
    cfg.mode = (i2s_mode_t)(I2S_MODE_MASTER | I2S_MODE_TX);
    cfg.sample_rate = 8000;
    cfg.bits_per_sample = I2S_BITS_PER_SAMPLE_16BIT;
    cfg.channel_format = I2S_CHANNEL_FMT_ONLY_LEFT;
    cfg.communication_format = I2S_COMM_FORMAT_STAND_I2S;
    cfg.intr_alloc_flags = 0;
    cfg.dma_buf_count = 8;
    cfg.dma_buf_len = samplesPerFrame;
    cfg.use_apll = false;
    cfg.tx_desc_auto_clear = true;

    esp_err_t err = i2s_driver_install(MVSR_SPK_I2S_PORT, &cfg, 0, NULL);
    if (err != ESP_OK) {
        LOG_ERROR("VoiceMemo: speaker I2S install failed: %d", err);
        return;
    }

    i2s_pin_config_t pins = {};
    pins.bck_io_num = MVSR_SPK_BCLK;
    pins.ws_io_num = MVSR_SPK_LRCLK;
    pins.data_out_num = MVSR_SPK_DATA;
    pins.data_in_num = I2S_PIN_NO_CHANGE;

    err = i2s_set_pin(MVSR_SPK_I2S_PORT, &pins);
    if (err != ESP_OK) {
        LOG_ERROR("VoiceMemo: speaker pin config failed: %d", err);
        i2s_driver_uninstall(MVSR_SPK_I2S_PORT);
        return;
    }

    i2s_start(MVSR_SPK_I2S_PORT);
    spkInitialized = true;
    LOG_INFO("VoiceMemo: Speaker initialized (BCLK=%d LRCLK=%d DATA=%d SD=%d)",
             MVSR_SPK_BCLK, MVSR_SPK_LRCLK, MVSR_SPK_DATA, MVSR_SPK_SD_MODE);
}

void VoiceMemoModule::deinitSpeaker()
{
    if (!spkInitialized) return;
    i2s_stop(MVSR_SPK_I2S_PORT);
    i2s_driver_uninstall(MVSR_SPK_I2S_PORT);
    digitalWrite(MVSR_SPK_SD_MODE, LOW);
    spkInitialized = false;
}

bool VoiceMemoModule::captureAndEncodeFrame()
{
    if (!micInitialized || !codec2 || !pcmBuffer) return false;

    size_t bytesRead = 0;
    esp_err_t err = i2s_read(MVSR_MIC_I2S_PORT, pcmBuffer,
                              samplesPerFrame * sizeof(int16_t),
                              &bytesRead, pdMS_TO_TICKS(100));

    if (err != ESP_OK || bytesRead < (size_t)(samplesPerFrame * sizeof(int16_t)))
        return false;

    encodeFrame(pcmBuffer);
    return true;
}

bool VoiceMemoModule::decodeAndPlayFrame()
{
    if (!spkInitialized || !codec2 || !pcmBuffer) return false;
    if (playOffset + codecBytesPerFrame > playBuffer.size()) return false;

    codec2_decode(codec2, pcmBuffer, playBuffer.data() + playOffset);
    playOffset += codecBytesPerFrame;

    size_t bytesWritten = 0;
    i2s_write(MVSR_SPK_I2S_PORT, pcmBuffer,
              samplesPerFrame * sizeof(int16_t),
              &bytesWritten, pdMS_TO_TICKS(500));
    return true;
}

#endif // HAS_VOICE_MEMO

// --- Core logic (works on any ESP32 with Codec2) ---

void VoiceMemoModule::encodeFrame(const int16_t *pcm)
{
    if (!codec2) return;
    size_t prev = recordBuffer.size();
    recordBuffer.resize(prev + codecBytesPerFrame);
    codec2_encode(codec2, recordBuffer.data() + prev, (short *)pcm);
}

bool VoiceMemoModule::startRecording(uint32_t maxDurationMs)
{
    if (state != State::IDLE) {
        LOG_WARN("VoiceMemo: busy (state=%d)", (int)state);
        return false;
    }
    if (!codec2) {
        initCodec2();
        if (!codec2) return false;
    }

    if (maxDurationMs > 60000) maxDurationMs = 60000;
    if (maxDurationMs < 1000) maxDurationMs = 1000;
    maxRecordMs = maxDurationMs;

    recordBuffer.clear();
    recordBuffer.reserve(maxDurationMs / 10); // ~100 B/s, overshoot OK
    recordStartTime = millis();
    state = State::RECORDING;

#ifdef HAS_VOICE_MEMO
    initMic();
#endif

    LOG_INFO("VoiceMemo: Recording started (max %u ms)", maxDurationMs);
    return true;
}

uint32_t VoiceMemoModule::stopAndSend(uint32_t destNodeId, uint8_t channelIndex)
{
    if (state != State::RECORDING) {
        LOG_WARN("VoiceMemo: not recording");
        return 0;
    }

    state = State::IDLE;
#ifdef HAS_VOICE_MEMO
    deinitMic();
#endif

    if (recordBuffer.empty()) {
        LOG_WARN("VoiceMemo: nothing recorded");
        return 0;
    }

    uint32_t durationSec = (millis() - recordStartTime + 500) / 1000;
    LOG_INFO("VoiceMemo: Stopped. %u bytes, ~%u s", recordBuffer.size(), durationSec);

    uint32_t checksum = crc32(recordBuffer.data(), recordBuffer.size());

    if (!mediaTransferModule) {
        LOG_ERROR("VoiceMemo: no MediaTransferModule");
        recordBuffer.clear();
        return 0;
    }

    uint32_t tid = mediaTransferModule->startTransfer(
        destNodeId, channelIndex,
        recordBuffer.data(), recordBuffer.size(),
        meshtastic_MediaContentType_VOICE_MEMO,
        checksum, "audio/codec2", durationSec);

    recordBuffer.clear();
    recordBuffer.shrink_to_fit();

    if (tid > 0)
        LOG_INFO("VoiceMemo: transfer %u to 0x%08x", tid, destNodeId);
    else
        LOG_ERROR("VoiceMemo: transfer start failed");

    return tid;
}

void VoiceMemoModule::cancelRecording()
{
    if (state != State::RECORDING) return;
    state = State::IDLE;
#ifdef HAS_VOICE_MEMO
    deinitMic();
#endif
    recordBuffer.clear();
    recordBuffer.shrink_to_fit();
    LOG_INFO("VoiceMemo: Recording cancelled");
}

bool VoiceMemoModule::playVoiceMemo(const uint8_t *data, uint32_t size)
{
    if (state != State::IDLE) {
        LOG_WARN("VoiceMemo: busy (state=%d)", (int)state);
        return false;
    }
    if (!codec2) {
        initCodec2();
        if (!codec2) return false;
    }
    if (size < (uint32_t)codecBytesPerFrame) {
        LOG_WARN("VoiceMemo: data too small");
        return false;
    }

    playBuffer.assign(data, data + size);
    playOffset = 0;
    state = State::PLAYING;

#ifdef HAS_VOICE_MEMO
    initSpeaker();
#endif

    uint32_t frames = size / codecBytesPerFrame;
    LOG_INFO("VoiceMemo: Playing %u frames (~%u ms)", frames, frames * 40);
    return true;
}

void VoiceMemoModule::stopPlayback()
{
    if (state != State::PLAYING) return;
    state = State::IDLE;
#ifdef HAS_VOICE_MEMO
    deinitSpeaker();
#endif
    playBuffer.clear();
    playBuffer.shrink_to_fit();
    playOffset = 0;
    LOG_INFO("VoiceMemo: Playback stopped");
}

void VoiceMemoModule::onTransferComplete(const uint8_t *data, uint32_t size, uint32_t fromNode)
{
    LOG_INFO("VoiceMemo: Received voice memo from 0x%08x (%u bytes)", fromNode, size);

#ifdef HAS_VOICE_MEMO
    // Auto-play on MVSR hardware
    playVoiceMemo(data, size);
#else
    uint32_t frames = (codecBytesPerFrame > 0) ? size / codecBytesPerFrame : 0;
    LOG_INFO("VoiceMemo: %u frames (~%u ms) — no speaker for playback", frames, frames * 40);
#endif
}

uint32_t VoiceMemoModule::generateAndSendTestMemo(uint32_t destNodeId, uint8_t channelIndex,
                                                    uint32_t durationMs)
{
    if (!codec2) {
        initCodec2();
        if (!codec2) return 0;
    }
    if (durationMs > 30000) durationMs = 30000;
    if (durationMs < 1000) durationMs = 1000;

    uint32_t totalFrames = durationMs / 40;
    recordBuffer.clear();
    recordBuffer.reserve(totalFrames * codecBytesPerFrame);

    int16_t *synth = new int16_t[samplesPerFrame];
    uint32_t sampleIdx = 0;

    for (uint32_t f = 0; f < totalFrames; f++) {
        for (int i = 0; i < samplesPerFrame; i++) {
            // 440 Hz sine at 8 kHz sample rate
            float t = (float)(sampleIdx + i) / 8000.0f;
            synth[i] = (int16_t)(16000.0f * sinf(2.0f * 3.14159265f * 440.0f * t));
        }
        sampleIdx += samplesPerFrame;
        encodeFrame(synth);
    }
    delete[] synth;

    LOG_INFO("VoiceMemo: Generated test memo — %u frames, %u bytes", totalFrames, recordBuffer.size());

    uint32_t checksum = crc32(recordBuffer.data(), recordBuffer.size());
    if (!mediaTransferModule) {
        LOG_ERROR("VoiceMemo: no MediaTransferModule");
        recordBuffer.clear();
        return 0;
    }

    uint32_t tid = mediaTransferModule->startTransfer(
        destNodeId, channelIndex,
        recordBuffer.data(), recordBuffer.size(),
        meshtastic_MediaContentType_VOICE_MEMO,
        checksum, "audio/codec2",
        (durationMs + 500) / 1000);

    recordBuffer.clear();
    recordBuffer.shrink_to_fit();

    if (tid > 0)
        LOG_INFO("VoiceMemo: test memo transfer %u to 0x%08x", tid, destNodeId);
    return tid;
}

float VoiceMemoModule::getRecordingDuration() const
{
    if (state != State::RECORDING) return 0.0f;
    return (float)(millis() - recordStartTime) / 1000.0f;
}

int32_t VoiceMemoModule::runOnce()
{
    if (!moduleConfig.has_media_transfer || !moduleConfig.media_transfer.enabled) {
        return disable();
    }

    switch (state) {
    case State::RECORDING: {
        if (millis() - recordStartTime >= maxRecordMs) {
            LOG_INFO("VoiceMemo: max duration reached (%u ms)", maxRecordMs);
            state = State::IDLE;
#ifdef HAS_VOICE_MEMO
            deinitMic();
#endif
            return 1000;
        }
#ifdef HAS_VOICE_MEMO
        captureAndEncodeFrame();
        return 5;
#else
        return 100;
#endif
    }

    case State::PLAYING: {
#ifdef HAS_VOICE_MEMO
        if (!decodeAndPlayFrame()) {
            LOG_INFO("VoiceMemo: Playback complete");
            stopPlayback();
            return 1000;
        }
        return 5;
#else
        stopPlayback();
        return 1000;
#endif
    }

    case State::IDLE:
    default:
        return 1000;
    }
}

uint32_t VoiceMemoModule::crc32(const uint8_t *data, uint32_t length)
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
